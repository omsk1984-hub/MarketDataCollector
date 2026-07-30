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
using System.Collections;
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
        private Channel<CollectedBatch> _batchChannel = null!;
        private readonly int _batchChannelCapacity;
        private readonly int _channelCapacity;
        private readonly int _flushIntervalSeconds;
        private readonly bool _useSingleConsumer;
        private readonly int _consumerCount;
        private readonly int _deduplicationCacheMaxSize;
        private readonly ITickAggregator? _tickAggregator;

        // Async Writer — Collector отправляет батчи Writer'у через отдельный канал
        private Task _writerTask = null!;

        // Adaptive batch size
        private readonly int _minBatchSize;
        private readonly int _maxBatchSize;
        private readonly int _backlogLowThreshold;
        private readonly int _backlogHighThreshold;
        private readonly double _writeDurationWarningMs;

        // Shared between Collector (reads) and Writer (writes) for adaptive batch size
        private long _lastWriteDurationMs;

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
        private CancellationTokenSource? _internalCts;
        private int _processedCount;
        private int _totalReceivedCount;
        private int _totalIncomingCount;
        private int _totalDroppedCount;
        private readonly Guid _sessionId = Guid.NewGuid();
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
            _minBatchSize = options.MinBatchSize;
            _maxBatchSize = options.MaxBatchSize > 0 ? options.MaxBatchSize : options.BatchSize;
            // Clamp: min не может быть больше max (защита от тестов с маленьким BatchSize)
            if (_minBatchSize > _maxBatchSize)
                _minBatchSize = _maxBatchSize;
            _batchChannelCapacity = options.BatchChannelCapacity;
            _channelCapacity = options.ChannelCapacity;
            _flushIntervalSeconds = options.FlushIntervalSeconds;
            _useSingleConsumer = options.UseSingleConsumer;
            _consumerCount = options.ConsumerCount;
            _deduplicationCacheMaxSize = options.DeduplicationCacheMaxSize;
            _backlogLowThreshold = options.BacklogLowThreshold;
            _backlogHighThreshold = options.BacklogHighThreshold;
            _writeDurationWarningMs = options.WriteDurationWarningMs;
            _processedCount = 0;
            _totalReceivedCount = 0;
            _totalIncomingCount = 0;
            _totalDroppedCount = 0;
            _tickAggregator = tickAggregator;

            // Default channel for ProcessTickAsync before StartProcessingAsync
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
            Interlocked.Increment(ref _totalIncomingCount);

            MarketDataTelemetry.TicksIncoming.Add(1, GetExchangeTag(exchange));

            LogProcessTickDebug(ticker, price, volume, exchange);

            var tick = new TickData(ticker, price, volume, timestamp, exchange);

            var channels = _channels;
            int channelIndex;
            if (_useSingleConsumer || channels.Length == 1)
            {
                channelIndex = 0;
            }
            else
            {
                channelIndex = (GetStableHashCode(ticker) & int.MaxValue) % channels.Length;
            }

            if (!channels[channelIndex].Writer.TryWrite(tick))
            {
                Interlocked.Increment(ref _totalDroppedCount);
                MarketDataTelemetry.TicksDropped.Add(1, GetExchangeTag(exchange));
            }

            if (_tickAggregator != null)
            {
                _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
            }

            return Task.CompletedTask;
        }

        public Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            for (int i = 0; i < _channels.Length; i++)
            {
                MarketDataTelemetry.ChannelFill.Record(
                    _channels[i].Reader.Count,
                    ChannelTag(i));
            }

            if (_processingTask != null && !_processingTask.IsCompleted)
                return _processingTask;

            if (_processingTask?.IsFaulted == true)
            {
                LogPreviousTaskFailed();
            }

            for (int i = 0; i < _channels.Length; i++)
            {
                var oldCount = _channels[i].Reader.Count;
                if (oldCount > 0)
                {
                    LogOldChannelHasData(_sessionId, i, oldCount);
                }
            }

            _internalCts?.Dispose();
            _internalCts = new CancellationTokenSource();
            var internalToken = _internalCts.Token;

            if (_useSingleConsumer)
            {
                // ===== Single Consumer Mode (Async Writer) =====
                _channels = new Channel<TickData>[]
                {
                    System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(_channelCapacity)
                    {
                        FullMode = BoundedChannelFullMode.DropOldest,
                        SingleReader = true,
                        SingleWriter = false
                    })
                };

                // Batch channel between Collector and Writer
                _batchChannel = System.Threading.Channels.Channel.CreateBounded<CollectedBatch>(
                    new BoundedChannelOptions(_batchChannelCapacity)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleReader = true,
                        SingleWriter = false
                    });

                // Writer (DB writes) + Collector (reads input, sends batches)
                _writerTask = WriterLoopAsync(channelIndex: 0, internalToken, _deduplicationCacheMaxSize);
                _processingTask = CollectorLoopAsync(channelIndex: 0, internalToken);

                LogSingleConsumerStart(_sessionId, _maxBatchSize, _channelCapacity);
            }
            else
            {
                // ===== Multiple Consumers Mode (legacy, unchanged) =====
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

                _channels = new Channel<TickData>[consumerCount];
                for (int i = 0; i < consumerCount; i++)
                {
                    _channels[i] = System.Threading.Channels.Channel.CreateBounded<TickData>(
                        new BoundedChannelOptions(_channelCapacity)
                        {
                            FullMode = BoundedChannelFullMode.DropOldest,
                            SingleReader = true,
                            SingleWriter = false
                        });
                }

                var tasks = new Task[consumerCount];
                for (int i = 0; i < consumerCount; i++)
                {
                    var channelIndex = i;
                    tasks[i] = ProcessBatchesAsync(channelIndex, internalToken, _deduplicationCacheMaxSize);
                }
                _processingTask = Task.WhenAll(tasks);

                LogMultiConsumerStart(_sessionId, consumerCount, countSource, _maxBatchSize, _channelCapacity);
            }

            return _processingTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            for (int i = 0; i < _channels.Length; i++)
            {
                MarketDataTelemetry.ChannelFill.Record(
                    _channels[i].Reader.Count,
                    ChannelTag(i));
            }

            var totalRemaining = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                totalRemaining += _channels[i].Reader.Count;
            }

            LogStopStatistics(_sessionId, totalRemaining, _totalIncomingCount, _totalReceivedCount, _processedCount);

            if (_useSingleConsumer)
            {
                // ===== Two-phase shutdown for Async Writer architecture =====
                // Phase 1: Complete input channel → Collector drains remaining ticks,
                //          sends any partial batch to batch channel → completes batch channel
                _channels[0].Writer.TryComplete();

                if (_processingTask != null)
                {
                    try
                    {
                        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        await _processingTask.WaitAsync(timeoutCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        LogShutdownTimeout(_sessionId);
                    }
                }

                // Phase 2: Wait for Writer to finish all pending batches
                if (_writerTask != null)
                {
                    try
                    {
                        using var writerTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                        await _writerTask.WaitAsync(writerTimeoutCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        LogShutdownTimeout(_sessionId);
                    }
                }
            }
            else
            {
                // ===== Legacy shutdown for multi-consumer mode =====
                for (int i = 0; i < _channels.Length; i++)
                {
                    _channels[i].Writer.TryComplete();
                }

                if (_processingTask != null)
                {
                    try
                    {
                        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        await _processingTask.WaitAsync(timeoutCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        LogShutdownTimeout(_sessionId);
                    }
                }
            }

            if (_internalCts != null)
            {
                try
                {
                    _internalCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            var totalIncoming = _totalIncomingCount;
            var totalReceived = _totalReceivedCount;
            var totalInserted = _processedCount;
            var totalDropped = _totalDroppedCount;
            var droppedByChannel = totalIncoming - totalReceived;
            var remainingAfterStop = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                remainingAfterStop += _channels[i].Reader.Count;
            }

            LogFinalStopStatistics(_sessionId, totalIncoming, totalReceived, totalInserted, totalDropped, droppedByChannel, remainingAfterStop);
        }

        // ========================================================================
        // Collector — читает тики из input channel, отправляет батчи Writer'у
        // Никогда не блокируется на БД
        // ========================================================================
        private async Task CollectorLoopAsync(int channelIndex, CancellationToken cancellationToken)
        {
            var channel = _channels[channelIndex];
            var adaptiveBatchSize = _minBatchSize;
            var batchArray = ArrayPool<TickData>.Shared.Rent(_maxBatchSize);
            int batchCount = 0;

            long lastWriteDurationMs = 0;

            var fillLevelTimer = Stopwatch.StartNew();
            const int fillLevelIntervalMs = 10_000;

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

                    Task<bool> readTask;
                    if (_flushIntervalSeconds > 0 && batchCount > 0)
                    {
                        var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        flushTimerCts.TryReset();
                        flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
                        var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
                        var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);

                        cancellationToken.ThrowIfCancellationRequested();

                        if (completed == flushDelay)
                        {
                            // Timer flush — send partial batch
                            LogTimerFlush(_sessionId, batchCount, adaptiveBatchSize, channelIndex);

                            var batch = new CollectedBatch { Items = batchArray, Count = batchCount };
                            await _batchChannel.Writer.WriteAsync(batch, cancellationToken);
                            batchArray = ArrayPool<TickData>.Shared.Rent(_maxBatchSize);
                            batchCount = 0;

                            adaptiveBatchSize = CalculateAdaptiveBatchSize(channel.Reader.Count, lastWriteDurationMs);
                            continue;
                        }

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
                    while (channel.Reader.TryRead(out var tick))
                    {
                        batchArray[batchCount++] = tick;
                        if (batchCount >= adaptiveBatchSize)
                        {
                            // Send full batch to Writer
                            var batch = new CollectedBatch { Items = batchArray, Count = batchCount };
                            await _batchChannel.Writer.WriteAsync(batch, cancellationToken);

                            // Rent new array, reset counter
                            batchArray = ArrayPool<TickData>.Shared.Rent(_maxBatchSize);
                            batchCount = 0;

                            // Recalculate adaptive batch size based on backlog
                            adaptiveBatchSize = CalculateAdaptiveBatchSize(channel.Reader.Count, _lastWriteDurationMs);

                            if (fillLevelTimer.ElapsedMilliseconds >= fillLevelIntervalMs)
                            {
                                MarketDataTelemetry.ChannelFill.Record(
                                    channel.Reader.Count,
                                    ChannelTag(channelIndex));
                                fillLevelTimer.Restart();
                            }
                        }
                    }

                    continue;

                channelCompleted:
                    if (batchCount > 0)
                    {
                        var batch = new CollectedBatch { Items = batchArray, Count = batchCount };
                        await _batchChannel.Writer.WriteAsync(batch, CancellationToken.None);
                        batchArray = ArrayPool<TickData>.Shared.Rent(0); // dummy, not used
                        batchCount = 0;

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
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not ChannelClosedException)
            {
                LogConsumerCriticalError(ex, _sessionId, channelIndex);
                throw;
            }
            finally
            {
                // If batchArray wasn't sent, return it to pool
                if (batchArray != null && batchArray.Length > 0)
                {
                    ArrayPool<TickData>.Shared.Return(batchArray, clearArray: false);
                }

                // Signal Writer that no more batches will be produced
                _batchChannel.Writer.TryComplete();
            }
        }

        // ========================================================================
        // Writer — читает батчи из batch channel, выполняет дедупликацию и BulkCopy в БД
        // ========================================================================
        private async Task WriterLoopAsync(int channelIndex, CancellationToken cancellationToken, int deduplicationCacheMaxSize)
        {
            var filteredSlice = new FilteredTickSlice(); // reusable
            var dedupCache = deduplicationCacheMaxSize > 0 ? new DeduplicationCache(deduplicationCacheMaxSize) : null;
            var fillLevelTimer = Stopwatch.StartNew();
            const int fillLevelIntervalMs = 10_000;

            try
            {
                await foreach (var batch in _batchChannel.Reader.ReadAllAsync(cancellationToken))
                {
                    var sw = Stopwatch.StartNew();
                    await ProcessBatchAsync(batch.Items, batch.Count, filteredSlice, dedupCache, cancellationToken, channelIndex);
                    sw.Stop();

                    // Track last write duration for adaptive batch size
                    _lastWriteDurationMs = (long)(sw.Elapsed.TotalMilliseconds * 1000); // store as microseconds

                    // Return array to pool — Writer owns it after receiving from Collector
                    ArrayPool<TickData>.Shared.Return(batch.Items, clearArray: false);

                    if (fillLevelTimer.ElapsedMilliseconds >= fillLevelIntervalMs)
                    {
                        MarketDataTelemetry.BatchChannelFill.Record(
                            _batchChannel.Reader.Count,
                            ChannelTag(channelIndex));
                        fillLevelTimer.Restart();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                LogChannelCancelled(_sessionId, channelIndex);
            }
            catch (ChannelClosedException)
            {
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not ChannelClosedException)
            {
                LogConsumerCriticalError(ex, _sessionId, channelIndex);
                // Worker observes Faulted writer and initiates shutdown
                throw;
            }
        }


        // ========================================================================
        // Адаптивный BatchSize
        // ========================================================================
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int CalculateAdaptiveBatchSize(int backlog, long lastWriteDurationUs)
        {
            int baseSize;

            if (backlog <= _backlogLowThreshold)
            {
                baseSize = _minBatchSize;
            }
            else if (backlog >= _backlogHighThreshold)
            {
                baseSize = _maxBatchSize;
            }
            else
            {
                var ratio = (backlog - _backlogLowThreshold) / (double)(_backlogHighThreshold - _backlogLowThreshold);
                baseSize = _minBatchSize + (int)(ratio * (_maxBatchSize - _minBatchSize));
            }

            // Reduce batch size if last write was too slow (prevent cascading backlog)
            if (_writeDurationWarningMs > 0 && lastWriteDurationUs > _writeDurationWarningMs * 1000)
            {
                baseSize = Math.Max(_minBatchSize, (int)(baseSize * 0.8));
            }

            MarketDataTelemetry.AdaptiveBatchSize.Record(baseSize);
            return baseSize;
        }

        /// <summary>
        /// Основной цикл обработки для одного consumer'а (Multi-Consumer mode, legacy).
        /// Каждый consumer работает со своим каналом _channels[channelIndex].
        /// </summary>
        private async Task ProcessBatchesAsync(int channelIndex, CancellationToken cancellationToken, int deduplicationCacheMaxSize)
        {
            var batchArray = ArrayPool<TickData>.Shared.Rent(Math.Max(_maxBatchSize, 1));
            var filteredSlice = new FilteredTickSlice();
            int batchCount = 0;
            var channel = _channels[channelIndex];

            var dedupCache = deduplicationCacheMaxSize > 0 ? new DeduplicationCache(deduplicationCacheMaxSize) : null;

            var fillLevelTimer = Stopwatch.StartNew();
            const int fillLevelIntervalMs = 10_000;

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

                    Task<bool> readTask;
                    if (_flushIntervalSeconds > 0 && batchCount > 0)
                    {
                        var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                        flushTimerCts.TryReset();
                        flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
                        var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
                        var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);

                        cancellationToken.ThrowIfCancellationRequested();

                        if (completed == flushDelay)
                        {
                            LogTimerFlush(_sessionId, batchCount, _maxBatchSize, channelIndex);

                            await ProcessBatchAsync(batchArray, batchCount, filteredSlice, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                            batchCount = 0;
                            continue;
                        }

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
                    while (channel.Reader.TryRead(out var tick))
                    {
                        batchArray[batchCount++] = tick;
                        if (batchCount >= _maxBatchSize)
                        {
                            await ProcessBatchAsync(batchArray, batchCount, filteredSlice, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                            batchCount = 0;

                            if (fillLevelTimer.ElapsedMilliseconds >= fillLevelIntervalMs)
                            {
                                MarketDataTelemetry.ChannelFill.Record(
                                    channel.Reader.Count,
                                    ChannelTag(channelIndex));
                                fillLevelTimer.Restart();
                            }
                        }
                    }

                    continue;

                channelCompleted:
                    if (batchCount > 0)
                    {
                        await ProcessBatchAsync(batchArray, batchCount, filteredSlice, dedupCache, cancellationToken, channelIndex).ConfigureAwait(false);
                        batchCount = 0;

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
            }
            catch (Exception ex) when (ex is not OperationCanceledException && ex is not ChannelClosedException)
            {
                LogConsumerCriticalError(ex, _sessionId, channelIndex);
                throw;
            }
            finally
            {
                if (batchCount > 0)
                {
                    LogFinalFlush(_sessionId, channelIndex, batchCount, _maxBatchSize);
                    await ProcessBatchAsync(batchArray, batchCount, filteredSlice, dedupCache, CancellationToken.None, channelIndex).ConfigureAwait(false);
                }
            }

            ArrayPool<TickData>.Shared.Return(batchArray, clearArray: false);
        }

        /// <summary>
        /// Reusable IReadOnlyList<TickData> wrapper over an array slice.
        /// Eliminates List<TickData> allocation per batch in the hot path.
        /// </summary>
        private sealed class FilteredTickSlice : IReadOnlyList<TickData>
        {
            private TickData[] _source = Array.Empty<TickData>();
            private int _count;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Set(TickData[] source, int count)
            {
                _source = source;
                _count = count;
            }

            public TickData this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _source[index];
            }

            public int Count => _count;

            public IEnumerator<TickData> GetEnumerator()
            {
                for (int i = 0; i < _count; i++)
                    yield return _source[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        /// <summary>
        /// Batch data transfer object between Collector and Writer.
        /// Collector fills from ArrayPool, sends to Writer, Writer processes and returns to pool.
        /// </summary>
        internal sealed class CollectedBatch
        {
            public TickData[] Items { get; set; } = null!;
            public int Count { get; set; }
        }

        /// <summary>
        /// Обрабатывает один батч тиков: дедупликация in-place + bulk insert.
        /// </summary>
        private async Task ProcessBatchAsync(TickData[] batchArray, int batchCount, FilteredTickSlice filteredSlice, DeduplicationCache? dedupCache, CancellationToken cancellationToken, int channelIndex = 0)
        {
            using var activity = MarketDataTelemetry.ActivitySource.StartActivity("ProcessBatch");
            activity?.SetTag("batch.size", batchCount);

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchSize = batchCount;

                MarketDataTelemetry.BatchSize.Record(batchSize);

                int cachedCount = 0;
                int writeIdx = 0;
                if (dedupCache != null)
                {
                    for (int i = 0; i < batchCount; i++)
                    {
                        var t = batchArray[i];
                        if (dedupCache.Contains(t.Ticker, t.Exchange, t.Timestamp))
                        {
                            cachedCount++;
                        }
                        else
                        {
                            batchArray[writeIdx++] = t;
                            dedupCache.Add(t.Ticker, t.Exchange, t.Timestamp);
                        }
                    }
                }
                else
                {
                    writeIdx = batchCount;
                }

                filteredSlice.Set(batchArray, writeIdx);

                activity?.SetTag("filtered.count", writeIdx);
                activity?.SetTag("cached.count", cachedCount);

                using var scope = _scopeFactory.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();

                var sw = Stopwatch.StartNew();
                var inserted = await repository.BulkCopyAsync(filteredSlice, _timeService, cancellationToken);
                sw.Stop();

                activity?.SetTag("inserted.count", inserted);

                MarketDataTelemetry.BatchWriteDuration.Record(
                    sw.Elapsed.TotalMilliseconds,
                    ChannelTag(channelIndex),
                    new KeyValuePair<string, object?>("batch_size", batchSize),
                    new KeyValuePair<string, object?>("inserted_count", inserted));

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

                MarketDataTelemetry.TicksReceived.Add(
                    batchSize,
                    ChannelTag(channelIndex));

                MarketDataTelemetry.TicksProcessed.Add(
                    inserted,
                    batchCount > 0 ? GetExchangeTag(batchArray[0].Exchange) : ExchangeUnknownTag);

                _processedRpsCounter.IncrementBatch(inserted);
                
                if (totalInserted % 10000 < inserted)
                {
                    LogPeriodicProgress(totalInserted, totalReceived, batchSize, writeIdx, cachedCount, inserted);
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
            }
        }

        public double GetProcessedRps() => _processedRpsCounter.GetRps();

        public Task<int> GetProcessedCountAsync()
        {
            return Task.FromResult(_processedCount);
        }

        public int GetTotalIncomingCount() => _totalIncomingCount;

        public int GetTotalReceivedCount() => _totalReceivedCount;

        public int GetTotalDroppedCount() => _totalDroppedCount;

        public int GetChannelCount()
        {
            int total = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                total += _channels[i].Reader.Count;
            }
            return total;
        }

        public int GetChannelCapacity() => _channelCapacity;

        public int GetConsumerCountChannels() => _channels.Length;

        public (int Count, int Capacity)[] GetChannelFillLevels()
        {
            var result = new (int Count, int Capacity)[_channels.Length];
            for (int i = 0; i < _channels.Length; i++)
            {
                result[i] = (_channels[i].Reader.Count, _channelCapacity);
            }
            return result;
        }

        public int GetEstimatedDroppedCount()
        {
            int droppedByChannel = _totalIncomingCount - _totalReceivedCount - GetChannelCount();
            return Math.Max(0, droppedByChannel);
        }

        public Channel<TickData> GetChannel(int index = 0) => _channels[index];
    }
}
