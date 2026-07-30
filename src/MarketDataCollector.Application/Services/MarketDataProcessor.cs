using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Core.Telemetry;
using MarketDataCollector.Core.Utilities;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using TickData = MarketDataCollector.Domain.Entities.TickData;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Buffers;

namespace MarketDataCollector.Application.Services
{
    public partial class MarketDataProcessor : IMarketDataProcessor
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
        private readonly int _deduplicationCacheMaxSize;
        private readonly ITickAggregator? _tickAggregator;

        // ========================================================================
        // Cached OTel tags — avoid KeyValuePair allocation per call in hot path
        // ========================================================================
        private static readonly KeyValuePair<string, object?> ExchangeTagBinance = new("exchange", "binance");
        private static readonly KeyValuePair<string, object?> ExchangeTagKraken = new("exchange", "kraken");
        private static readonly KeyValuePair<string, object?> ExchangeUnknownTag = new("exchange", "unknown");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static KeyValuePair<string, object?> GetExchangeTag(string exchange)
            => exchange switch
            {
                "Binance" => ExchangeTagBinance,
                "Kraken" => ExchangeTagKraken,
                _ => new KeyValuePair<string, object?>("exchange", exchange)
            };

        // Pre-allocated channel_index tags (up to 16 channels is more than enough)
        private static readonly KeyValuePair<string, object?>[] ChannelIndexTags =
            Enumerable.Range(0, 16).Select(i => new KeyValuePair<string, object?>("channel_index", i)).ToArray();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static KeyValuePair<string, object?> ChannelTag(int index)
            => (uint)index < (uint)ChannelIndexTags.Length
                ? ChannelIndexTags[index]
                : new KeyValuePair<string, object?>("channel_index", index);

        // Cached exception type and sql_state tags
        private static readonly KeyValuePair<string, object?> ExceptionTypePostgresTag = new("exception_type", "PostgresException");
        private static readonly KeyValuePair<string, object?> ExceptionTypeNpgsqlTag = new("exception_type", "NpgsqlException");
        private static readonly KeyValuePair<string, object?> SqlStateNoneTag = new("sql_state", "none");

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
            _deduplicationCacheMaxSize = options.DeduplicationCacheMaxSize;
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

            // OpenTelemetry: счётчик входящих тиков
            MarketDataTelemetry.TicksIncoming.Add(1, GetExchangeTag(exchange));

            // TryWrite — неблокирующая запись. При переполнении канала (BoundedChannelFullMode.DropOldest)
            // возвращает false без исключения. Считаем такие случаи как реальные дропы.
            // Это точнее, чем вычислять разницу incoming - received постфактум,
            // т.к. received обновляется с задержкой (после формирования и обработки батча).
            var tick = new TickData(ticker, price, volume, timestamp, exchange);

            // Определяем канал для записи:
            // - SingleConsumer mode: всегда канал 0
            // - Multiple consumers mode: round-robin по атомарному счётчику,
            //   чтобы нагрузка распределялась равномерно между всеми consumer'ами.
            //   Это решает проблему хэш-коллизий, когда несколько быстрых тикеров
            //   попадают в один канал, вызывая переполнение и DropOldest.
            var channels = _channels;
            int channelIndex;
            if (_useSingleConsumer || channels.Length == 1)
            {
                channelIndex = 0;
            }
            else
            {
                // Per-ticker routing: детерминированный хэш от ticker'а.
                // Гарантирует, что все тики одного тикера попадают в один канал,
                // а разные consumer'ы работают с disjoint наборами тикеров.
                // Это исключает deadlock'и (40P01) при ON CONFLICT DO NOTHING,
                // т.к. два consumer'а никогда не конкурируют за один unique index.
                //
                // При 3 тикерах и 3 consumer'ах нагрузка распределяется идеально.
                // При асимметричной нагрузке (один тикер быстрее других)
                // DropOldest защищает от переполнения канала.
                channelIndex = (GetStableHashCode(ticker) & int.MaxValue) % channels.Length;
            }

            if (!channels[channelIndex].Writer.TryWrite(tick))
            {
                Interlocked.Increment(ref _totalDroppedCount);
                // OpenTelemetry: счётчик дропнутых тиков
                MarketDataTelemetry.TicksDropped.Add(1, GetExchangeTag(exchange));
            }

            // Передаём тик в агрегатор (если он подключён) — fire-and-forget,
            // чтобы агрегатор не блокировал основной пайплайн.
            // Канал агрегатора использует DropOldest, поэтому при перегрузке
            // старые тики отбрасываются, а не блокируется producer.
            if (_tickAggregator != null)
            {
                _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
            }

            return Task.CompletedTask;
        }

        public Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            // OpenTelemetry: записываем fill-метрики каналов при старте
            for (int i = 0; i < _channels.Length; i++)
            {
                MarketDataTelemetry.ChannelFill.Record(
                    _channels[i].Reader.Count,
                    ChannelTag(i));
            }

            if (_processingTask != null && !_processingTask.IsCompleted)
                return _processingTask;

            // Логируем ошибку предыдущей задачи, если она завершилась с ошибкой
            if (_processingTask?.IsFaulted == true)
            {
                LogPreviousTaskFailed();
            }

            // Диагностика: проверяем, не осталось ли данных от предыдущих каналов
            // (например, если этот метод был вызван повторно, или клиенты начали
            // писать данные до старта процессора).
            for (int i = 0; i < _channels.Length; i++)
            {
                var oldCount = _channels[i].Reader.Count;
                if (oldCount > 0)
                {
                    LogOldChannelHasData(_sessionId, i, oldCount);
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

                _processingTask = ProcessBatchesAsync(channelIndex: 0, internalToken, _deduplicationCacheMaxSize);

                LogSingleConsumerStart(_sessionId, _batchSize, _channelCapacity);
            }
            else
            {
                // ===== Multiple Consumers Mode =====
                // Создаём отдельные каналы для каждого consumer'а с SingleReader=true.
                // Per-ticker routing в ProcessTickAsync гарантирует, что каждый consumer
                // получает disjoint набор тикеров (детерминированный хэш ticker'а).
                // B-tree страницы unique-индекса (ticker, exchange, timestamp)
                // физически не пересекаются — deadlock'и (40P01) невозможны.

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
                    tasks[i] = ProcessBatchesAsync(channelIndex, internalToken, _deduplicationCacheMaxSize);
                }
                _processingTask = Task.WhenAll(tasks);

                LogMultiConsumerStart(_sessionId, consumerCount, countSource, _batchSize, _channelCapacity);
            }

            return _processingTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            // OpenTelemetry: финальная запись fill-метрик перед остановкой
            for (int i = 0; i < _channels.Length; i++)
            {
                MarketDataTelemetry.ChannelFill.Record(
                    _channels[i].Reader.Count,
                    ChannelTag(i));
            }

            // 1. Логируем остаток во всех каналах перед TryComplete, чтобы оценить,
            //    сколько тиков будет дочитано.
            var totalRemaining = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                totalRemaining += _channels[i].Reader.Count;
            }

            LogStopStatistics(_sessionId, totalRemaining, _totalIncomingCount, _totalReceivedCount, _processedCount);

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
                    LogShutdownTimeout(_sessionId);
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

            LogFinalStopStatistics(_sessionId, totalIncoming, totalReceived, totalInserted, totalDropped, droppedByChannel, remainingAfterStop);
        }

        /// <summary>
        /// Основной цикл обработки для одного consumer'а.
        /// Каждый consumer работает со своим каналом _channels[channelIndex].
        /// </summary>
        private async Task ProcessBatchesAsync(int channelIndex, CancellationToken cancellationToken, int deduplicationCacheMaxSize)
        {
            // Use ArrayPool instead of List<TickData> — eliminates List allocation per consumer
            var batchArray = ArrayPool<TickData>.Shared.Rent(Math.Max(_batchSize, 1));
            int batchCount = 0;
            var channel = _channels[channelIndex];

            // Per-consumer кэш дедупликации — каждый consumer обрабатывает disjoint набор тикеров,
            // поэтому синхронизация не нужна.
            var dedupCache = deduplicationCacheMaxSize > 0 ? new DeduplicationCache(deduplicationCacheMaxSize) : null;

            // Для периодической записи fill level (раз в ~10 сек)
            var fillLevelTimer = Stopwatch.StartNew();
            const int fillLevelIntervalMs = 10_000;

            // Timer для принудительного сброса частичных батчей (переиспользуется вместо Task.Delay).
            // Создаётся один раз на весь lifecycle consumer'а — устраняет ~600+ internal timer creations.
            using var flushTimerCts = new CancellationTokenSource();
            Timer? flushTimer = null;
            if (_flushIntervalSeconds > 0)
            {
                flushTimer = new Timer(_ => flushTimerCts.Cancel(),
                    null, Timeout.Infinite, Timeout.Infinite);
            }

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Ждём новый тик в канале данных.
                    // Если настроен таймер сброса частичных батчей и батч непустой —
                    // используем Timer для принудительного сброса (без создания Task.Delay per iteration).
                    Task<bool> readTask;
                    if (_flushIntervalSeconds > 0 && batchCount > 0)
                    {
                        var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        // Переиспользуем один Timer: сбрасываем предыдущее состояние и ставим новый таймаут
                        flushTimerCts.TryReset();
                        flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
                        var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
                        var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);

                        cancellationToken.ThrowIfCancellationRequested();

                        if (completed == flushDelay)
                        {
                            // --- Сброс частичного батча по таймеру ---
                            LogTimerFlush(_sessionId, batchCount, _batchSize, channelIndex);

                            await ProcessBatchAsync(batchArray, batchCount, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                            batchCount = 0;
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
                        batchArray[batchCount++] = tick;
                        if (batchCount >= _batchSize)
                        {
                            await ProcessBatchAsync(batchArray, batchCount, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                            batchCount = 0;

                            // Периодическая запись fill level (раз в ~10 сек)
                            if (fillLevelTimer.ElapsedMilliseconds >= fillLevelIntervalMs)
                            {
                                MarketDataTelemetry.ChannelFill.Record(
                                    channel.Reader.Count,
                                    ChannelTag(channelIndex));
                                fillLevelTimer.Restart();
                            }
                        }
                    }

                    // Продолжаем цикл — ждём новые тики или сброс по таймеру
                    continue;

                channelCompleted:
                    // Канал данных завершён (TryComplete) — выходим из цикла
                    // Перед выходом сбросим частичный батч
                    if (batchCount > 0)
                    {
                        await ProcessBatchAsync(batchArray, batchCount, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                        batchCount = 0;

                        // Периодическая запись fill level (раз в ~10 сек)
                        if (fillLevelTimer.ElapsedMilliseconds >= fillLevelIntervalMs)
                        {
                            MarketDataTelemetry.ChannelFill.Record(
                                channel.Reader.Count,
                                ChannelTag(channelIndex));
                            fillLevelTimer.Restart();
                        }
                    }
                    break;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                LogChannelCancelled(_sessionId, channelIndex);
            }
            catch (ChannelClosedException)
            {
                // Ожидаемо при завершении канала
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not ChannelClosedException)
            {
                LogConsumerCriticalError(ex, _sessionId, channelIndex);
                // finally выполнит финальный flush с CancellationToken.None,
                // затем исключение пробросится → _processingTask станет Faulted.
                // Worker observe'ит IsFaulted и инициирует graceful shutdown.
                throw;
            }
            finally
            {
                // Финальный flush (даже при ошибке — CancellationToken.None)
                // Важно: не вызываем ProcessBatchAsync повторно, если уже сбросили выше
                if (batchCount > 0)
                {
                    LogFinalFlush(_sessionId, channelIndex, batchCount, _batchSize);
                    await ProcessBatchAsync(batchArray, batchCount, dedupCache, CancellationToken.None, channelIndex).ConfigureAwait(false);
                }
            }
        }

        private async Task ProcessBatchAsync(TickData[] batchArray, int batchCount, DeduplicationCache? dedupCache, CancellationToken cancellationToken, int channelIndex = 0)
        {
            // OpenTelemetry: трейсинг обработки батча
            using var activity = MarketDataTelemetry.ActivitySource.StartActivity("ProcessBatch");
            activity?.SetTag("batch.size", batchCount);

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchSize = batchCount;

                // OpenTelemetry: гистограмма размера батча
                MarketDataTelemetry.BatchSize.Record(batchSize);

                // 1. Единый проход: фильтрация через кэш дедупликации.
                //    Кэш заполняется РАНЬШЕ DB-вставки — поэтому ловит
                //    и intra-batch дубли (те же ключи в одном батче),
                //    и cross-batch дубли (ключи из предыдущих батчей).
                // Use ArrayPool for filteredTicks too — eliminates List<TickData> allocation
                List<TickData>? filteredList = null;
                int cachedCount = 0;
                if (dedupCache != null)
                {
                    filteredList = new List<TickData>(batchCount);
                    for (int i = 0; i < batchCount; i++)
                    {
                        var t = batchArray[i];
                        if (dedupCache.Contains(t.Ticker, t.Exchange, t.Timestamp))
                        {
                            cachedCount++;
                        }
                        else
                        {
                            filteredList.Add(t);
                            dedupCache.Add(t.Ticker, t.Exchange, t.Timestamp);
                        }
                    }
                }
                else
                {
                    // Кэш отключён — простой проход без выделения новой коллекции,
                    // передаём batchArray напрямую (срез по batchCount)
                    filteredList = new List<TickData>(batchCount);
                    for (int i = 0; i < batchCount; i++)
                    {
                        filteredList.Add(batchArray[i]);
                    }
                }

                activity?.SetTag("filtered.count", filteredList.Count);
                activity?.SetTag("cached.count", cachedCount);

                // 2. Создаём отдельный scope для DbContext — каждый consumer получает свой экземпляр,
                //    чтобы избежать InvalidOperationException при параллельном доступе из нескольких потоков
                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();

                // 3. Bulk insert напрямую из filteredList (без промежуточного List<RawTick>)
                var sw = Stopwatch.StartNew();
                var inserted = await repository.BulkCopyAsync(filteredList, _timeService, cancellationToken);
                sw.Stop();

                activity?.SetTag("inserted.count", inserted);

                // OpenTelemetry: гистограмма времени записи батча
                // (batch_size и inserted_count варьируются — не кэшируются)
                MarketDataTelemetry.BatchWriteDuration.Record(
                    sw.Elapsed.TotalMilliseconds,
                    ChannelTag(channelIndex),
                    new KeyValuePair<string, object?>("batch_size", batchSize),
                    new KeyValuePair<string, object?>("inserted_count", inserted));

                // Single Consumer mode: гонки нет — обычные присваивания быстрее Interlocked.
                // Multiple Consumers mode: Interlocked гарантирует атомарность.
                int totalReceived, totalInserted;
                if (_useSingleConsumer)
                {
                    _totalReceivedCount += batchSize;
                    _processedCount += inserted;
                    totalReceived = _totalReceivedCount;
                    totalInserted = _processedCount;
                }
                else
                {
                    totalReceived = Interlocked.Add(ref _totalReceivedCount, batchSize);
                    totalInserted = Interlocked.Add(ref _processedCount, inserted);
                }

                // OpenTelemetry: счётчики полученных и обработанных тиков
                MarketDataTelemetry.TicksReceived.Add(
                    batchSize,
                    ChannelTag(channelIndex));

                MarketDataTelemetry.TicksProcessed.Add(
                    inserted,
                    batchCount > 0 ? GetExchangeTag(batchArray[0].Exchange) : ExchangeUnknownTag);

                // Batch increment: один Interlocked.Add вместо N Interlocked.Increment
                _processedRpsCounter.IncrementBatch(inserted);
                
                if (totalInserted % 10000 < inserted)
                {
                    LogPeriodicProgress(totalInserted, totalReceived, batchSize, filteredList.Count, cachedCount, inserted);
                }
            }
            catch (OperationCanceledException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Cancelled");
                LogBatchCancelled();
                throw;
            }
            catch (PostgresException pgEx)
            {
                activity?.SetStatus(ActivityStatusCode.Error, pgEx.Message);
                activity?.SetTag("exception.type", "PostgresException");
                activity?.SetTag("exception.sql_state", pgEx.SqlState);
                LogPostgresError(pgEx, pgEx.SqlState, batchCount, channelIndex);
                MarketDataTelemetry.ExceptionsByType.Add(1,
                    ExceptionTypePostgresTag,
                    new KeyValuePair<string, object?>("sql_state", pgEx.SqlState));
            }
            catch (NpgsqlException npgEx)
            {
                activity?.SetStatus(ActivityStatusCode.Error, npgEx.Message);
                activity?.SetTag("exception.type", "NpgsqlException");
                LogNpgsqlError(npgEx, batchCount, channelIndex);
                MarketDataTelemetry.ExceptionsByType.Add(1,
                    ExceptionTypeNpgsqlTag,
                    SqlStateNoneTag);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.SetTag("exception.type", ex.GetType().Name);
                LogUnexpectedBatchError(ex, batchCount, channelIndex);
                MarketDataTelemetry.ExceptionsByType.Add(1,
                    new KeyValuePair<string, object?>("exception_type", ex.GetType().Name),
                    SqlStateNoneTag);
                // Временная ошибка (БД, сеть и т.д.) — consumer продолжает работать.
                // Исключение НЕ пробрасывается, чтобы следующие батчи обрабатывались.
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
        /// Заполненность каждого канала: (Count, Capacity) по каждому consumer'у.
        /// </summary>
        public (int Count, int Capacity)[] GetChannelFillLevels()
        {
            var result = new (int Count, int Capacity)[_channels.Length];
            for (int i = 0; i < _channels.Length; i++)
            {
                result[i] = (_channels[i].Reader.Count, _channelCapacity);
            }
            return result;
        }

        /// <summary>
        /// Оценка реально дропнутых тиков через DropOldest.
        /// Считается как max(0, incoming - received - channelCount).
        /// </summary>
        public int GetEstimatedDroppedCount()
        {
            int droppedByChannel = _totalIncomingCount - _totalReceivedCount - GetChannelCount();
            return Math.Max(0, droppedByChannel);
        }

        /// <summary>
        /// Доступ к каналу по индексу (для тестов).
        /// В production код не используется — routing происходит внутри ProcessTickAsync.
        /// SingleConsumer mode: index=0.
        /// </summary>
        public Channel<TickData> GetChannel(int index = 0) => _channels[index];
    }
}
