using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Core.Utilities;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MarketDataCollector.Application.Services
{
    public class MarketDataProcessor : IMarketDataProcessor
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<MarketDataProcessor> _logger;
        private readonly ITimeService _timeService;
        private Channel<TickData> _channel = null!;
        public Channel<TickData> Channel => _channel;
        private readonly int _batchSize;
        private readonly int _channelCapacity;
        private readonly bool _useSingleConsumer;
        private readonly ITickAggregator? _tickAggregator;

        private Task _processingTask = null!;
        private int _processedCount;       // сколько реально вставлено в БД (после ON CONFLICT DO NOTHING)
        private int _totalReceivedCount;   // сколько всего тиков пришло в ProcessBatchAsync (до дедупликации)
        private readonly SlidingWindowCounter _processedRpsCounter = new();

        public event EventHandler<Exception>? OnError;

        public readonly record struct TickData(
            string Ticker,
            decimal Price,
            decimal Volume,
            DateTime Timestamp,
            string Exchange
        );

        public MarketDataProcessor(
            IServiceScopeFactory scopeFactory,
            ILogger<MarketDataProcessor> logger,
            ITimeService timeService,
            MarketDataProcessorOptions options,
            ITickAggregator? tickAggregator = null)
        {
            ArgumentNullException.ThrowIfNull(scopeFactory);
            ArgumentNullException.ThrowIfNull(options);

            _scopeFactory = scopeFactory;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _timeService = timeService ?? throw new ArgumentNullException(nameof(timeService));
            _batchSize = options.BatchSize;
            _channelCapacity = options.ChannelCapacity;
            _useSingleConsumer = options.UseSingleConsumer;
            _processedCount = 0;
            _totalReceivedCount = 0;
            _tickAggregator = tickAggregator;

            // Создаём канал по умолчанию (режим multiple consumers), чтобы ProcessTickAsync
            // мог безопасно писать до вызова StartProcessingAsync.
            // В StartProcessingAsync канал будет пересоздан с правильными параметрами
            // (SingleReader=true/false) перед запуском consumer'ов.
            _channel = System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(_channelCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = false,
                SingleWriter = false
            });
        }

        public async Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
        {
            await _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange));

            // Передаём тик в агрегатор (если он подключён) — fire-and-forget,
            // чтобы агрегатор не блокировал основной пайплайн.
            // Канал агрегатора использует DropOldest, поэтому при перегрузке
            // старые тики отбрасываются, а не блокируется producer.
            if (_tickAggregator != null)
            {
                _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
            }

            _logger.LogDebug("Тик добавлен в очередь: {Ticker} {Price} {Volume} {Exchange}", ticker, price, volume, exchange);
        }

        public Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            if (_processingTask != null && !_processingTask.IsCompleted)
                return Task.CompletedTask;

            // Логируем ошибку предыдущей задачи, если она завершилась с ошибкой
            if (_processingTask?.IsFaulted == true)
            {
                _logger.LogError(_processingTask.Exception?.InnerException ?? _processingTask.Exception,
                    "Предыдущая задача обработки завершилась ошибкой, перезапуск");
            }

            if (_useSingleConsumer)
            {
                // ===== Single Consumer Mode =====
                // Пересоздаём Channel с SingleReader=true — гарантия, что только один поток
                // читает из канала. Полностью исключает конкуренцию за BulkCopyLock семафор
                // и deadlock'и (40P01) на уровне БД.
                //
                // По результатам бенчмарка: Sequential batch=700 даёт ~62 680 ticks/sec,
                // что достаточно для текущей нагрузки.
                // writer.TryComplete() на старом канале не требуется — он не использовался,
                // т.к. StartProcessingAsync вызывается до ProcessTickAsync.
                _channel = System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(_channelCapacity)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = true,
                    SingleWriter = false
                });

                _processingTask = ProcessBatchesAsync(cancellationToken);

                _logger.LogInformation(
                    "Обработчик рыночных данных запущен: Single Consumer mode, batchSize={BatchSize}, ChannelCapacity={Capacity}",
                    _batchSize, _channelCapacity);
            }
            else
            {
                // ===== Multiple Consumers Mode (default) =====
                // Канал уже создан в конструкторе с SingleReader=false.
                // Запускаем несколько parallel consumer'ов, которые конкурентно читают из Channel.
                //
                // Сама вставка в БД (BulkCopyAsync) сериализована через SemaphoreSlim(1,1)
                // в RawTickRepository.
                //
                // По результатам бенчмарка: 4 consumer'а с чанком 800 дают
                // ~50-55k ticks/sec.

                var consumerCount = Math.Clamp((int)Math.Ceiling(Environment.ProcessorCount / 2.0), 1, 4);
                var consumers = Enumerable.Range(0, consumerCount)
                    .Select(_ => ProcessBatchesAsync(cancellationToken));
                _processingTask = Task.WhenAll(consumers);

                _logger.LogInformation(
                    "Обработчик рыночных данных запущен: {ConsumerCount} consumer'ов, batchSize={BatchSize}, ChannelCapacity={Capacity}",
                    consumerCount, _batchSize, _channelCapacity);
            }

            return Task.CompletedTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            _channel.Writer.TryComplete();
            
            if (_processingTask != null)
            {
                try
                {
                    await _processingTask.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Остановка обработки отменена");
                }
            }
            
            _logger.LogInformation("Обработчик рыночных данных остановлен. Всего обработано: {Count}", _processedCount);
        }

        private async Task ProcessBatchesAsync(CancellationToken cancellationToken)
        {
            var batch = new List<TickData>(_batchSize);

            try
            {
                await foreach (var tick in _channel.Reader.ReadAllAsync(cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    batch.Add(tick);

                    if (batch.Count >= _batchSize)
                    {
                        await ProcessBatchAsync(batch, cancellationToken);
                        batch.Clear();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Обработка отменена");
            }
            catch (ChannelClosedException)
            {
                // Ожидаемо при завершении канала
            }
            finally
            {
                // Финальный flush
                if (batch.Count > 0)
                {
                    await ProcessBatchAsync(batch, CancellationToken.None);
                }
            }
        }

        private async Task ProcessBatchAsync(List<TickData> batch, CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchSize = batch.Count;

                // 1. Убираем дубликаты в памяти
                var uniqueTicks = batch
                    .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))
                    .Select(g => g.First())
                    .ToList();

                // 2. Создаём отдельный scope для DbContext — каждый consumer получает свой экземпляр,
                //    чтобы избежать InvalidOperationException при параллельном доступе из нескольких потоков
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();

                // 3. Bulk insert через Npgsql Binary COPY protocol (быстрее ExecuteSqlRaw в 10-100x)
                var entities = uniqueTicks.Select(t => new RawTick(
                    t.Ticker, t.Price, t.Volume, t.Timestamp, t.Exchange, _timeService
                )).ToList();

                var inserted = await repository.BulkCopyAsync(entities, cancellationToken);

                var totalReceived = Interlocked.Add(ref _totalReceivedCount, batchSize);
                var totalInserted = Interlocked.Add(ref _processedCount, inserted);

                // Инкрементируем RPS-счётчик для каждого сохранённого тика
                for (int i = 0; i < inserted; i++)
                {
                    _processedRpsCounter.Increment();
                }
                
                if (totalInserted % 1000 < inserted)
                {
                    _logger.LogInformation(
                        "Всего обработано: {TotalInserted} вставлено, {TotalReceived} получено (batch={BatchSize}, uniq={Unique}, вставлено={Inserted})",
                        totalInserted, totalReceived, batchSize, uniqueTicks.Count, inserted);
                }

                _logger.LogDebug("Батч сохранён: {Saved} вставлено, {Duplicates} дубликатов пропущено",
                    inserted, entities.Count - inserted);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Обработка батча отменена");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Критическая ошибка при обработке батча из {Count} тиков", batch.Count);
                OnError?.Invoke(this, ex);
            }
        }

        public double GetProcessedRps() => _processedRpsCounter.GetRps();

        public Task<int> GetProcessedCountAsync()
        {
            return Task.FromResult(_processedCount);
        }
    }
}
