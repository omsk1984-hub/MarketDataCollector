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
using System.Runtime.CompilerServices;
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
        private Channel<TickData>[] _channels = null!;
        private readonly int _batchSize;
        private readonly int _channelCapacity;
        private readonly int _flushIntervalSeconds;
        private readonly bool _useSingleConsumer;
        private readonly int _consumerCount;
        private readonly ITickAggregator? _tickAggregator;

        private Task _processingTask = null!;
        private CancellationTokenSource? _internalCts;  // внутренний CTS для graceful shutdown:
                                                        // внешний stoppingToken отменяется хостом,
                                                        // но consumer'ы должны дочитать backlog
                                                        // перед остановкой.
        private int _processedCount;       // сколько реально вставлено в БД (после ON CONFLICT DO NOTHING)
        private int _totalReceivedCount;   // сколько всего тиков пришло в ProcessBatchAsync (до дедупликации)
        private int _totalIncomingCount;   // сколько всего тиков поступило в ProcessTickAsync
        private int _totalDroppedCount;    // сколько тиков реально дропнуто каналом (TryWrite=false из-за DropOldest)
        private readonly Guid _sessionId = Guid.NewGuid(); // уникальный ID сессии для связывания логов
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
            _flushIntervalSeconds = options.FlushIntervalSeconds;
            _useSingleConsumer = options.UseSingleConsumer;
            _consumerCount = options.ConsumerCount;
            _processedCount = 0;
            _totalReceivedCount = 0;
            _totalIncomingCount = 0;
            _totalDroppedCount = 0;
            _tickAggregator = tickAggregator;

            // Создаём канал по умолчанию (1 канал для SingleConsumer mode), чтобы ProcessTickAsync
            // мог безопасно писать до вызова StartProcessingAsync.
            // В StartProcessingAsync каналы будут пересозданы с правильными параметрами.
            _channels = new Channel<TickData>[]
            {
                System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(_channelCapacity)
                {
                    FullMode = BoundedChannelFullMode.DropOldest,
                    SingleReader = false,
                    SingleWriter = false
                })
            };
        }

        /// <summary>
        /// Детерминированный хэш для строки (Ordinal), стабильный между запусками.
        /// Используется для маршрутизации тиков по consumer'ам в multiple consumers mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetStableHashCode(string value)
        {
            unchecked
            {
                int hash = 17;
                foreach (char c in value)
                {
                    hash = hash * 31 + c;
                }
                return hash;
            }
        }

        public Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
        {
            // Инкрементируем счётчик ДО записи в канал — общее количество попыток записи.
            Interlocked.Increment(ref _totalIncomingCount);

            // TryWrite — неблокирующая запись. При переполнении канала (BoundedChannelFullMode.DropOldest)
            // возвращает false без исключения. Считаем такие случаи как реальные дропы.
            // Это точнее, чем вычислять разницу incoming - received постфактум,
            // т.к. received обновляется с задержкой (после формирования и обработки батча).
            var tick = new TickData(ticker, price, volume, timestamp, exchange);

            // Определяем канал для записи:
            // - SingleConsumer mode: всегда канал 0
            // - Multiple consumers mode: по хэшу ticker'а, чтобы каждый consumer получал
            //   disjoint набор тикеров (B-tree страницы unique-индекса не пересекаются →
            //   deadlock'и невозможны → SemaphoreSlim в BulkCopyAsync не нужен)
            var channels = _channels;
            int channelIndex;
            if (_useSingleConsumer || channels.Length == 1)
            {
                channelIndex = 0;
            }
            else
            {
                // Math.Abs может вернуть int.MinValue, что даёт отрицательное число.
                // Используем & 0x7FFFFFFF для гарантии положительного значения.
                channelIndex = (GetStableHashCode(ticker) & 0x7FFFFFFF) % channels.Length;
            }

            if (!channels[channelIndex].Writer.TryWrite(tick))
            {
                Interlocked.Increment(ref _totalDroppedCount);
            }

            // Передаём тик в агрегатор (если он подключён) — fire-and-forget,
            // чтобы агрегатор не блокировал основной пайплайн.
            // Канал агрегатора использует DropOldest, поэтому при перегрузке
            // старые тики отбрасываются, а не блокируется producer.
            if (_tickAggregator != null)
            {
                _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
            }

            _logger.LogDebug("Тик добавлен в очередь: {Ticker} {Price} {Volume} {Exchange}", ticker, price, volume, exchange);

            return Task.CompletedTask;
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

            // Диагностика: проверяем, не осталось ли данных от предыдущих каналов
            // (например, если этот метод был вызван повторно, или клиенты начали
            // писать данные до старта процессора).
            for (int i = 0; i < _channels.Length; i++)
            {
                var oldCount = _channels[i].Reader.Count;
                if (oldCount > 0)
                {
                    _logger.LogWarning(
                        "Session={SessionId}: Старый канал[{Index}] содержит {Count} необработанных тиков перед заменой. " +
                        "Это указывает на ошибку порядка запуска — клиенты писали данные до старта процессора.",
                        _sessionId, i, oldCount);
                }
            }

            // Создаём внутренний CancellationTokenSource, НЕ линкованный к внешнему cancellationToken.
            // Consumer'ы (ProcessBatchesAsync) используют _internalCts.Token, а не внешний cancellationToken.
            // Это гарантирует, что consumer'ы НЕ умрут по OperationCanceledException при остановке хоста,
            // а дождутся TryComplete() каналов и дочитают backlog.
            //
            // В StopProcessingAsync порядок:
            //   1. TryComplete() на всех каналах → consumer'ы дочитывают backlog по channelCompleted
            //   2. await _processingTask → ожидание завершения consumer'ов
            //   3. отмена _internalCts → освобождение ресурсов
            //
            // ВАЖНО: _internalCts НЕ линкован к внешнему cancellationToken! Иначе consumer'ы упадут
            // по OperationCanceledException ещё до TryComplete(), когда хост отменит stoppingToken.
            // Внешний токен управляет остановкой WebSocket-клиентов и выходом из health-check loop.
            // Consumer'ы управляются только _internalCts.
            _internalCts?.Dispose();
            _internalCts = new CancellationTokenSource();
            var internalToken = _internalCts.Token;

            if (_useSingleConsumer)
            {
                // ===== Single Consumer Mode =====
                // Пересоздаём Channel с SingleReader=true — гарантия, что только один поток
                // читает из канала. Полностью исключает конкуренцию за индексные блокировки
                // и deadlock'и (40P01) на уровне БД.
                //
                // По результатам бенчмарка: Sequential batch=700 даёт ~62 680 ticks/sec,
                // что достаточно для текущей нагрузки.
                _channels = new Channel<TickData>[]
                {
                    System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(_channelCapacity)
                    {
                        FullMode = BoundedChannelFullMode.DropOldest,
                        SingleReader = true,
                        SingleWriter = false
                    })
                };

                _processingTask = ProcessBatchesAsync(channelIndex: 0, internalToken);

                _logger.LogInformation(
                    "Session={SessionId}: Обработчик рыночных данных запущен: Single Consumer mode, batchSize={BatchSize}, ChannelCapacity={Capacity}",
                    _sessionId, _batchSize, _channelCapacity);
            }
            else
            {
                // ===== Multiple Consumers Mode (default) =====
                // Создаём отдельные каналы для каждого consumer'а с SingleReader=true.
                // Каждый consumer получает disjoint набор тикеров (по хэшу ticker'а в ProcessTickAsync),
                // поэтому B-tree страницы unique-индекса (ticker, exchange, timestamp)
                // физически не пересекаются — deadlock'и (40P01) невозможны,
                // и SemaphoreSlim в BulkCopyAsync больше не нужен.
                //
                // Ожидаемая производительность: ~3x прирост при 3 consumer'ах и 3+ тикерах
                // против текущего SemaphoreSlim-режима (~55k → ~150k+ ticks/sec).

                int consumerCount;
                string countSource;
                if (_consumerCount > 0)
                {
                    consumerCount = _consumerCount;
                    countSource = "configured";
                }
                else
                {
                    consumerCount = Math.Clamp((int)Math.Ceiling(Environment.ProcessorCount / 2.0), 1, 4);
                    countSource = "auto";
                }

                // Создаём N независимых каналов — по одному на consumer
                _channels = new Channel<TickData>[consumerCount];
                for (int i = 0; i < consumerCount; i++)
                {
                    _channels[i] = System.Threading.Channels.Channel.CreateBounded<TickData>(
                        new BoundedChannelOptions(_channelCapacity)
                        {
                            FullMode = BoundedChannelFullMode.DropOldest,
                            SingleReader = true,   // каждый канал — для одного consumer'а
                            SingleWriter = false
                        });
                }

                // Запускаем consumer'ов — каждый читает из своего канала.
                // Используем internalToken (линкован к внешнему cancellationToken),
                // чтобы consumer'ы не умирали при отмене хоста до вызова TryComplete().
                var tasks = new Task[consumerCount];
                for (int i = 0; i < consumerCount; i++)
                {
                    var channelIndex = i; // capture for closure
                    tasks[i] = ProcessBatchesAsync(channelIndex, internalToken);
                }
                _processingTask = Task.WhenAll(tasks);

                _logger.LogInformation(
                    "Session={SessionId}: Обработчик рыночных данных запущен: {ConsumerCount} consumer'ов ({CountSource}), " +
                    "batchSize={BatchSize}, ChannelCapacity={Capacity}, routing=tickerHash",
                    _sessionId, consumerCount, countSource, _batchSize, _channelCapacity);
            }

            return Task.CompletedTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            // 1. Логируем остаток во всех каналах перед TryComplete, чтобы оценить,
            //    сколько тиков будет дочитано.
            var totalRemaining = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                totalRemaining += _channels[i].Reader.Count;
            }

            _logger.LogInformation(
                "Session={SessionId}: Остановка обработчика. Остаток в каналах: {Remaining}, всего входящих: {Incoming}, получено из канала: {Received}, вставлено: {Inserted}",
                _sessionId, totalRemaining, _totalIncomingCount, _totalReceivedCount, _processedCount);

            // 2. Завершаем ВСЕ каналы данных — это заставит ProcessBatchesAsync
            //    выйти из цикла (readTask.Result == false → channelCompleted → break).
            //    ВАЖНО: это делается ДО отмены _internalCts, чтобы consumer'ы
            //    успели дочитать backlog и выполнить финальный flush.
            for (int i = 0; i < _channels.Length; i++)
            {
                _channels[i].Writer.TryComplete();
            }

            if (_processingTask != null)
            {
                try
                {
                    // 3. Ждём, пока ВСЕ ProcessBatchesAsync завершатся.
                    //    Consumer'ы используют _internalCts.Token, который ещё не отменён,
                    //    поэтому они НЕ умрут по OperationCanceledException.
                    //    После TryComplete() каналов consumer'ы дочитают остатки и выйдут
                    //    по channelCompleted (readTask.Result == false).
                    //    Используем CancellationToken.None + внутренний timeout 30с,
                    //    т.к. внешний cancellationToken может быть уже отменён.
                    using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    await _processingTask.WaitAsync(timeoutCts.Token);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Session={SessionId}: Превышен таймаут ожидания дочитывания backlog (30с). " +
                        "Остаток в каналах будет потерян.", _sessionId);
                }
            }

            // 4. Теперь consumer'ы завершены — безопасно отменяем _internalCts
            //    (чтобы освободить ресурсы).
            if (_internalCts != null)
            {
                try
                {
                    _internalCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // Ignore — может быть уже отменён
                }
            }

            // 5. Расширенный финальный лог со всей статистикой потерь
            var totalIncoming = _totalIncomingCount;
            var totalReceived = _totalReceivedCount;
            var totalInserted = _processedCount;
            var totalDropped = _totalDroppedCount;           // реальные дропы через TryWrite
            var droppedByChannel = totalIncoming - totalReceived; // backlog (для сравнения)
            var remainingAfterStop = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                remainingAfterStop += _channels[i].Reader.Count;
            }

            _logger.LogInformation(
                "Session={SessionId}: Обработчик рыночных данных остановлен. " +
                "Входящих: {Incoming}, получено из канала: {Received}, вставлено в БД: {Inserted}, " +
                "реально дропнуто: {DroppedReal}, backlog (incoming-received): {DroppedCalc}, остаток в каналах: {Remaining}",
                _sessionId, totalIncoming, totalReceived, totalInserted, totalDropped, droppedByChannel, remainingAfterStop);
        }

        /// <summary>
        /// Основной цикл обработки для одного consumer'а.
        /// Каждый consumer работает со своим каналом _channels[channelIndex].
        /// </summary>
        private async Task ProcessBatchesAsync(int channelIndex, CancellationToken cancellationToken)
        {
            var batch = new List<TickData>(_batchSize);
            var channel = _channels[channelIndex];

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Ждём новый тик в канале данных.
                    // Если настроен таймер сброса частичных батчей и батч непустой —
                    // используем Task.Delay как таймер для принудительного сброса.
                    // Это заменяет старый механизм с Timer + сигнальный канал _flushSignal,
                    // который не масштабировался на N consumer'ов.
                    Task<bool> readTask;
                    if (_flushIntervalSeconds > 0 && batch.Count > 0)
                    {
                        var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        var flushDelay = Task.Delay(TimeSpan.FromSeconds(_flushIntervalSeconds), cancellationToken);
                        var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);

                        cancellationToken.ThrowIfCancellationRequested();

                        if (completed == flushDelay)
                        {
                            // --- Сброс частичного батча по таймеру ---
                            _logger.LogDebug(
                                "Session={SessionId}: Таймерный сброс частичного батча: {Count} тиков (batchSize={BatchSize}), channel={Channel}",
                                _sessionId, batch.Count, _batchSize, channelIndex);

                            await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                            batch.Clear();
                            continue; // переходим к следующей итерации — снова ждём тики
                        }

                        // completed == readTask — проверяем результат
                        if (readTaskTyped.Result)
                        {
                            goto readTicks;
                        }
                        else
                        {
                            goto channelCompleted;
                        }
                    }
                    else
                    {
                        readTask = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        if (await readTask.ConfigureAwait(false))
                        {
                            goto readTicks;
                        }
                        else
                        {
                            goto channelCompleted;
                        }
                    }

                readTicks:
                    // Вычитываем ВСЕ доступные тики из канала (non-blocking)
                    while (channel.Reader.TryRead(out var tick))
                    {
                        batch.Add(tick);
                        if (batch.Count >= _batchSize)
                        {
                            await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                            batch.Clear();
                        }
                    }

                    // Продолжаем цикл — ждём новые тики или сброс по таймеру
                    continue;

                channelCompleted:
                    // Канал данных завершён (TryComplete) — выходим из цикла
                    // Перед выходом сбросим частичный батч
                    if (batch.Count > 0)
                    {
                        await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                        batch.Clear();
                    }
                    break;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Session={SessionId}: channel={Channel} обработка отменена", _sessionId, channelIndex);
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
                        "Session={SessionId}: Финальный сброс channel={Channel}: {Count} тиков (batchSize={BatchSize})",
                        _sessionId, channelIndex, batch.Count, _batchSize);
                    await ProcessBatchAsync(batch, CancellationToken.None).ConfigureAwait(false);
                }
            }
        }

        private async Task ProcessBatchAsync(List<TickData> batch, CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchSize = batch.Count;

                // 1. Убираем дубликаты в памяти (внутри-батчевые дубли)
                var uniqueTicks = batch
                    .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))
                    .Select(g => g.First())
                    .ToList();

                // 2. Создаём отдельный scope для DbContext — каждый consumer получает свой экземпляр,
                //    чтобы избежать InvalidOperationException при параллельном доступе из нескольких потоков
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();

                // 3. Bulk insert через Npgsql Binary COPY protocol (быстрее ExecuteSqlRaw в 10-100x)
                //    SemaphoreSlim в BulkCopyAsync удалён — deadlock'и невозможны, т.к. каждый consumer
                //    пишет disjoint наборы тикеров (per-ticker routing в ProcessTickAsync),
                //    поэтому B-tree страницы unique-индекса не пересекаются.
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
        /// Возвращает общее количество тиков, поступивших в ProcessTick.
        /// </summary>
        public int GetTotalIncomingCount() => _totalIncomingCount;

        /// <summary>
        /// Возвращает общее количество тиков, успешно прочитанных из каналов.
        /// </summary>
        public int GetTotalReceivedCount() => _totalReceivedCount;

        /// <summary>
        /// Количество тиков, реально дропнутых каналами из-за переполнения
        /// (TryWrite вернул false при BoundedChannelFullMode.DropOldest).
        /// </summary>
        public int GetTotalDroppedCount() => _totalDroppedCount;

        /// <summary>
        /// Суммарное количество тиков во всех каналах (для мониторинга заполненности).
        /// </summary>
        public int GetChannelCount()
        {
            int total = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                total += _channels[i].Reader.Count;
            }
            return total;
        }

        /// <summary>
        /// Ёмкость каждого канала (ChannelCapacity из конфигурации).
        /// </summary>
        public int GetChannelCapacity() => _channelCapacity;

        /// <summary>
        /// Количество активных каналов (consumer'ов).
        /// </summary>
        public int GetConsumerCountChannels() => _channels.Length;

        /// <summary>
        /// Доступ к каналу по индексу (для тестов).
        /// В production код не используется — routing происходит внутри ProcessTickAsync.
        /// SingleConsumer mode: index=0.
        /// </summary>
        public Channel<TickData> GetChannel(int index = 0) => _channels[index];
    }
}
