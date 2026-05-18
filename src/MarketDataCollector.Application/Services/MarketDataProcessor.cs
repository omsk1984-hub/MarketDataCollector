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
        private readonly Channel<TickData> _channel;
        public Channel<TickData> Channel => _channel;
        private readonly int _batchSize;
        private readonly ITickAggregator? _tickAggregator;

        private Task _processingTask = null!;
        private int _processedCount;
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
            int batchSize,
            int channelCapacity,
            ITickAggregator? tickAggregator = null)
        {
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _logger = logger;
            _timeService = timeService;
            _batchSize = batchSize;
            _processedCount = 0;
            _tickAggregator = tickAggregator;

            _channel = System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
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

            // Запускаем один consumer (SingleReader=true), который последовательно читает,
            // набирает батч и пишет в БД. Это устраняет необходимость в SemaphoreSlim
            // и исключает contention между несколькими consumer'ами.
            _processingTask = ProcessBatchesAsync(cancellationToken);
            _logger.LogInformation("Обработчик рыночных данных запущен: 1 consumer, batchSize={_batchSize}, FullMode=DropOldest",
                _batchSize);
            
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

                var count = Interlocked.Add(ref _processedCount, inserted);

                // Инкрементируем RPS-счётчик для каждого сохранённого тика
                for (int i = 0; i < inserted; i++)
                {
                    _processedRpsCounter.Increment();
                }
                
                if (count % 100 < inserted)
                {
                    _logger.LogInformation("Всего обработано тиков: {Count}", count);
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
