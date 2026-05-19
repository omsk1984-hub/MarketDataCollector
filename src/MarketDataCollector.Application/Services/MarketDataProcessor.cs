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
        private readonly int _flushIntervalSeconds;
        private readonly bool _useSingleConsumer;
        private readonly ITickAggregator? _tickAggregator;

        private Task _processingTask = null!;
        private int _processedCount;       // сколько реально вставлено в БД (после ON CONFLICT DO NOTHING)
        private int _totalReceivedCount;   // сколько всего тиков пришло в ProcessBatchAsync (до дедупликации)
        private int _totalIncomingCount;   // сколько всего тиков поступило в ProcessTickAsync (до Channel DropOldest)
        private readonly Guid _sessionId = Guid.NewGuid(); // уникальный ID сессии для связывания логов
        private readonly SlidingWindowCounter _processedRpsCounter = new();

        // Таймерный сброс частичных батчей
        private readonly Channel<byte> _flushSignal;   // сигнальный канал для таймера
        private Timer? _flushTimer;

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
            _flushIntervalSeconds = options.FlushIntervalSeconds;
            _useSingleConsumer = options.UseSingleConsumer;
            _processedCount = 0;
            _totalReceivedCount = 0;
            _totalIncomingCount = 0;
            _tickAggregator = tickAggregator;

            // Создаём сигнальный канал для таймерного сброса частичных батчей.
            // Ёмкость 1, SingleReader=true (только ProcessBatchesAsync читает).
            // DropOldest — если таймер сработал повторно до обработки сигнала,
            // старый сигнал отбрасывается, что безопасно (сброс будет в следующем цикле).
            _flushSignal = System.Threading.Channels.Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false
            });

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
            // Инкрементируем счётчик ДО записи в канал, чтобы можно было вычислить,
            // сколько тиков дропнуто при переполнении (DropOldest).
            Interlocked.Increment(ref _totalIncomingCount);

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
                    "Session={SessionId}: Обработчик рыночных данных запущен: Single Consumer mode, batchSize={BatchSize}, ChannelCapacity={Capacity}",
                    _sessionId, _batchSize, _channelCapacity);
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
                    "Session={SessionId}: Обработчик рыночных данных запущен: {ConsumerCount} consumer'ов, batchSize={BatchSize}, ChannelCapacity={Capacity}",
                    _sessionId, consumerCount, _batchSize, _channelCapacity);
            }

            // Запускаем таймер для принудительного сброса частичных батчей при простое.
            // Таймер пишет 1 байт в сигнальный канал _flushSignal.
            // ProcessBatchesAsync через Task.WhenAny ждёт либо новые тики, либо сигнал сброса.
            if (_flushIntervalSeconds > 0)
            {
                _flushTimer?.Dispose();
                _flushTimer = new Timer(
                    static state =>
                    {
                        var signal = (Channel<byte>)state!;
                        signal.Writer.TryWrite(0);
                    },
                    _flushSignal,
                    TimeSpan.FromSeconds(_flushIntervalSeconds),
                    TimeSpan.FromSeconds(_flushIntervalSeconds));

                _logger.LogInformation(
                    "Session={SessionId}: Таймер сброса частичных батчей запущен: каждые {FlushInterval}с, batchSize={BatchSize}",
                    _sessionId, _flushIntervalSeconds, _batchSize);
            }

            return Task.CompletedTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            // 1. Останавливаем таймер — он больше не будет писать в сигнальный канал
            _flushTimer?.Dispose();
            _flushTimer = null;

            // 2. Логируем остаток в канале перед TryComplete, чтобы оценить,
            //    сколько тиков не успеет обработаться.
            var remainingInChannel = _channel.Reader.Count;
            _logger.LogInformation(
                "Session={SessionId}: Остановка обработчика. Остаток в канале данных: {Remaining}, всего входящих: {Incoming}, получено из канала: {Received}, вставлено: {Inserted}",
                _sessionId, remainingInChannel, _totalIncomingCount, _totalReceivedCount, _processedCount);

            // 3. Завершаем канал данных — это заставит ProcessBatchesAsync
            //    выйти из цикла (readTask.Result == false → break).
            //    ВАЖНО: завершаем ДО сигнального канала, чтобы ProcessBatchesAsync
            //    корректно дочитал оставшиеся тики и выполнил финальный flush.
            _channel.Writer.TryComplete();

            if (_processingTask != null)
            {
                try
                {
                    // 4. Ждём, пока ProcessBatchesAsync завершится (дочитает остатки и выйдет)
                    await _processingTask.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Session={SessionId}: Остановка обработки отменена", _sessionId);
                }
            }

            // 5. После остановки processing task — завершаем сигнальный канал
            _flushSignal.Writer.TryComplete();

            // 6. Расширенный финальный лог со всей статистикой потерь
            var totalIncoming = _totalIncomingCount;
            var totalReceived = _totalReceivedCount;
            var totalInserted = _processedCount;
            var droppedByChannel = totalIncoming - totalReceived;
            var remainingAfterStop = _channel.Reader.Count;

            _logger.LogInformation(
                "Session={SessionId}: Обработчик рыночных данных остановлен. " +
                "Входящих: {Incoming}, получено из канала: {Received}, вставлено в БД: {Inserted}, " +
                "дропнуто каналом: {Dropped}, остаток в канале: {Remaining}",
                _sessionId, totalIncoming, totalReceived, totalInserted, droppedByChannel, remainingAfterStop);
        }

        private async Task ProcessBatchesAsync(CancellationToken cancellationToken)
        {
            var batch = new List<TickData>(_batchSize);
            var flushReader = _flushSignal.Reader;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Ждём: либо новый тик в канале данных, либо сигнал от таймера сброса
                    var readTask = _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                    var flushTask = flushReader.WaitToReadAsync(cancellationToken).AsTask();
                    var completed = await Task.WhenAny(readTask, flushTask);

                    cancellationToken.ThrowIfCancellationRequested();

                    var isReadCompleted = completed == readTask;
                    var isFlushCompleted = completed == flushTask;

                    // --- Обработка новых тиков ---
                    if (isReadCompleted)
                    {
                        if (readTask.Result)
                        {
                            // Вычитываем ВСЕ доступные тики из канала (non-blocking)
                            while (_channel.Reader.TryRead(out var tick))
                            {
                                batch.Add(tick);
                                if (batch.Count >= _batchSize)
                                {
                                    await ProcessBatchAsync(batch, cancellationToken);
                                    batch.Clear();
                                }
                            }
                        }
                        else
                        {
                            // Канал данных завершён (TryComplete) — выходим из цикла
                            // Перед выходом сбросим частичный батч
                            if (batch.Count > 0)
                            {
                                await ProcessBatchAsync(batch, cancellationToken);
                                batch.Clear();
                            }
                            break;
                        }
                    }

                    // --- Сброс частичного батча по сигналу таймера ---
                    if (batch.Count > 0 && isFlushCompleted)
                    {
                        // Потребляем сигнал из канала
                        while (flushReader.TryRead(out _)) { }

                        _logger.LogDebug(
                            "Таймерный сброс частичного батча: {Count} тиков (batchSize={BatchSize})",
                            batch.Count, _batchSize);

                        await ProcessBatchAsync(batch, cancellationToken);
                        batch.Clear();
                    }

                    // --- Потребляем "зависшие" сигналы из канала сброса ---
                    // Это необходимо, если таймер успел сработать несколько раз до обработки,
                    // а данные пришли раньше сигнала сброса (isReadCompleted=true, isFlushCompleted=false).
                    // Если в канале сброса есть данные — потребляем их, чтобы не накапливались.
                    if (isReadCompleted && !isFlushCompleted)
                    {
                        while (flushReader.TryRead(out _)) { }
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Обработка отменена");
            }
            catch (ChannelClosedException)
            {
                // Ожидаемо при завершении канала
            }
            finally
            {
                // Финальный flush (даже при ошибке — CancellationToken.None)
                // Важно: не вызываем ProcessBatchAsync повторно, если уже сбросили выше
                if (batch.Count > 0)
                {
                    _logger.LogDebug(
                        "Финальный сброс: {Count} тиков (batchSize={BatchSize})",
                        batch.Count, _batchSize);
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
                        "Session={SessionId}: Всего обработано: {TotalInserted} вставлено, {TotalReceived} получено (batch={BatchSize}, uniq={Unique}, вставлено={Inserted})",
                        _sessionId, totalInserted, totalReceived, batchSize, uniqueTicks.Count, inserted);
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

        /// <summary>
        /// Возвращает общее количество тиков, поступивших в ProcessTickAsync.
        /// </summary>
        public int GetTotalIncomingCount() => _totalIncomingCount;

        /// <summary>
        /// Возвращает общее количество тиков, успешно прочитанных из канала.
        /// </summary>
        public int GetTotalReceivedCount() => _totalReceivedCount;
    }
}
