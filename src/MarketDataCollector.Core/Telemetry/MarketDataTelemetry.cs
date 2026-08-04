using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace MarketDataCollector.Core.Telemetry;

/// <summary>
/// Централизованный источник метрик (Meter) и трейсинга (ActivitySource) для OpenTelemetry.
/// Все инструменты — статические, чтобы минимизировать overhead в hot path.
/// Теги: exchange, symbol, channel_index — позволяют фильтровать и агрегировать в Aspire Dashboard.
/// </summary>
public static class MarketDataTelemetry
{
    /// <summary>
    /// Имя Meter'а — регистрируется в Program.cs через AddMeter("MarketDataCollector").
    /// </summary>
    public const string MeterName = "MarketDataCollector";

    /// <summary>
    /// Имя ActivitySource — для трейсинга бизнес-операций.
    /// </summary>
    public const string ActivitySourceName = "MarketDataCollector";

    /// <summary>
    /// Версия инструментов.
    /// </summary>
    public const string Version = "1.0.0";

    /// <summary>
    /// Глобальный экземпляр Meter.
    /// </summary>
    public static readonly Meter Instance = new(MeterName, Version);

    /// <summary>
    /// Глобальный экземпляр ActivitySource.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new(ActivitySourceName, Version);

    // ========================================================================
    // Metrics — Counters
    // ========================================================================

    /// <summary>
    /// Количество сообщений, полученных от WebSocket (на входе в систему).
    /// Теги: exchange, symbol
    /// </summary>
    public static readonly Counter<long> WsMessagesReceived = Instance.CreateCounter<long>(
        name: "ws.messages.received",
        unit: "count",
        description: "Total WebSocket messages received from exchange");

    /// <summary>
    /// Количество тиков, поступивших в ProcessTickAsync (до записи в Channel).
    /// Теги: exchange
    /// </summary>
    public static readonly Counter<long> TicksIncoming = Instance.CreateCounter<long>(
        name: "ticks.incoming",
        unit: "count",
        description: "Total ticks entering the processor pipeline");

    /// <summary>
    /// Количество тиков, успешно извлечённых из Channel и поступивших в батч.
    /// Теги: channel_index
    /// </summary>
    public static readonly Counter<long> TicksReceived = Instance.CreateCounter<long>(
        name: "ticks.received",
        unit: "count",
        description: "Total ticks read from channel into batch");

    /// <summary>
    /// Количество тиков, успешно вставленных в БД (после дедупликации).
    /// Теги: exchange
    /// </summary>
    public static readonly Counter<long> TicksProcessed = Instance.CreateCounter<long>(
        name: "ticks.processed",
        unit: "count",
        description: "Total ticks successfully written to database");

    /// <summary>
    /// Количество тиков, отсеянных in-process DeduplicationCache внутри батча.
    /// Теги: channel_index
    /// </summary>
    public static readonly Counter<long> TicksDeduplicatedByCache = Instance.CreateCounter<long>(
        name: "ticks.deduplicated.cache",
        unit: "count",
        description: "Ticks filtered by in-process DeduplicationCache within a batch");

    /// <summary>
    /// Количество тиков, отсеянных на уровне БД (ON CONFLICT DO NOTHING).
    /// Теги: channel_index
    /// </summary>
    public static readonly Counter<long> TicksDeduplicatedByDb = Instance.CreateCounter<long>(
        name: "ticks.deduplicated.db",
        unit: "count",
        description: "Ticks skipped by database ON CONFLICT DO NOTHING");

    // TicksDropped — ObservableGauge вместо Counter, чтобы сэмпл был виден даже при нуле.
    // Значение накапливается в Interlocked-аккумуляторах по 3 exchange-тегам (без lock и аллокаций),
    // а экспортёр читает их при каждом сборе (см. IncrementTicksDropped). Имя остаётся ticks.dropped.
    private static long _ticksDroppedBinance;
    private static long _ticksDroppedKraken;
    private static long _ticksDroppedOther;

    /// <summary>
    /// Количество тиков, дропнутых каналом (TryWrite=false из-за DropOldest).
    /// ObservableGauge — экспортируется при каждом сборе, даже когда значение 0.
    /// Теги: exchange
    /// </summary>
    public static readonly ObservableGauge<long> TicksDropped = Instance.CreateObservableGauge(
        name: "ticks.dropped",
        observeValues: () => new[]
        {
            new Measurement<long>(Volatile.Read(ref _ticksDroppedBinance),
                new KeyValuePair<string, object?>("exchange", "binance")),
            new Measurement<long>(Volatile.Read(ref _ticksDroppedKraken),
                new KeyValuePair<string, object?>("exchange", "kraken")),
            new Measurement<long>(Volatile.Read(ref _ticksDroppedOther),
                new KeyValuePair<string, object?>("exchange", "unknown"))
        },
        unit: "count",
        description: "Total ticks dropped due to channel overflow");

    // ========================================================================
    // Metrics — UpDownCounters (текущее состояние)
    // ========================================================================

    /// <summary>
    /// Текущее количество активных WebSocket-соединений.
    /// Теги: exchange
    /// +1 при подключении, -1 при отключении.
    /// </summary>
    public static readonly UpDownCounter<long> ActiveConnections = Instance.CreateUpDownCounter<long>(
        name: "ws.active_connections",
        unit: "count",
        description: "Current number of active WebSocket connections");

    // ========================================================================
    // Metrics — Histograms (распределение значений)
    // ========================================================================

    /// <summary>
    /// Распределение размера батча при записи в БД.
    /// </summary>
    public static readonly Histogram<long> BatchSize = Instance.CreateHistogram<long>(
        name: "ticks.batch.size",
        unit: "count",
        description: "Distribution of batch sizes when writing to database");

    /// <summary>
    /// Распределение заполненности Channel (количество тиков в очереди).
    /// </summary>
    public static readonly Histogram<long> ChannelFill = Instance.CreateHistogram<long>(
        name: "processor.channel.fill",
        unit: "count",
        description: "Channel fill level (current queue depth)");

    /// <summary>
    /// Длительность записи батча в БД (гистограмма).
    /// Теги: channel_index, batch_size, inserted_count
    /// </summary>
    public static readonly Histogram<double> BatchWriteDuration = Instance.CreateHistogram<double>(
        name: "ticks.batch.write.duration",
        unit: "ms",
        description: "Duration of batch database write operation");

    // ========================================================================
    // Metrics — Channel monitoring (new)
    // ========================================================================

    /// <summary>
    /// Текущий fill level по каждому каналу (instant). ObservableGauge — экспортируется
    /// при каждом сборе, даже когда значение 0 (в отличие от UpDownCounter, который
    /// не создаёт инструмент при нулевой дельте). Значения обновляет Worker.
    /// Теги: channel_index
    /// </summary>
    private static readonly ConcurrentDictionary<int, long> ChannelFillLevelValues = new();

    public static void SetChannelFillLevel(int channelIndex, long value)
    {
        ChannelFillLevelValues[channelIndex] = value;
    }

    public static readonly ObservableGauge<long> ChannelFillLevel = Instance.CreateObservableGauge(
        name: "processor.channel.fill_level",
        observeValues: () => ChannelFillLevelValues.Select(kv => new Measurement<long>(
            kv.Value,
            new KeyValuePair<string, object?>("channel_index", kv.Key))),
        unit: "count",
        description: "Current ticks in channel by channel_index");

    // TicksDroppedSilently — ObservableGauge вместо Counter, чтобы сэмпл был виден даже при нуле.
    // Текущее накопленное значение выставляет Worker в health-check цикле через SetTicksDroppedSilently,
    // экспортёр читает его при каждом сборе. Имя остаётся ticks.dropped.silently.
    private static long _ticksDroppedSilently;

    public static void SetTicksDroppedSilently(long value) => Volatile.Write(ref _ticksDroppedSilently, value);

    /// <summary>
    /// Оценка дропнутых тиков через DropOldest (current value).
    /// ObservableGauge — экспортируется при каждом сборе, даже когда значение 0.
    /// </summary>
    public static readonly ObservableGauge<long> TicksDroppedSilently = Instance.CreateObservableGauge(
        name: "ticks.dropped.silently",
        observeValues: () => new[]
        {
            new Measurement<long>(Volatile.Read(ref _ticksDroppedSilently))
        },
        unit: "count",
        description: "Estimated ticks dropped silently by DropOldest mode");

    /// <summary>
    /// Backlog канала (incoming - received) — мгновенное давление.
    /// </summary>
    public static readonly UpDownCounter<long> ChannelBacklog = Instance.CreateUpDownCounter<long>(
        name: "processor.channel.backlog",
        unit: "count",
        description: "Channel backlog (incoming - received)");

    // ========================================================================
    // Metrics — Async Writer / Adaptive Batch (new)
    // ========================================================================

    /// <summary>
    /// Fill level batch channel (количество батчей в очереди на запись).
    /// </summary>
    public static readonly Histogram<long> BatchChannelFill = Instance.CreateHistogram<long>(
        name: "processor.batch_channel.fill",
        unit: "count",
        description: "Batch channel fill level (batches pending write)");

    /// <summary>
    /// Адаптивный batch size (текущее значение).
    /// </summary>
    public static readonly Histogram<long> AdaptiveBatchSize = Instance.CreateHistogram<long>(
        name: "ticks.batch.adaptive_size",
        unit: "count",
        description: "Current adaptive batch size value");

    // ========================================================================
    // Metrics — Exception tracking (new)
    // ========================================================================

    /// <summary>
    /// Счётчик исключений по типам для real-time мониторинга в Prometheus.
    /// Теги: exception_type, sql_state
    /// Инкрементируется в catch-блоках MarketDataProcessor и RawTickRepository.
    /// </summary>
    public static readonly Counter<double> ExceptionsByType = Instance.CreateCounter<double>(
        name: "exceptions_total",
        description: "Total exceptions by type");

    // ========================================================================
    // Metrics — Batched Counters (reduce OTel internal lock contention)
    //
    // Per-message Counters (TicksIncoming, WsMessagesReceived, TicksDropped)
    // инкрементируются в hot path через CounterBatcher (Interlocked.Increment,
    // без lock и аллокаций), а реальный Counter.Add выносится один раз за батч
    // вызовом FlushMetricBatchers() из writer loop.
    //
    // Имена/теги/единицы метрик НЕ меняются — меняется только частота Add.
    // ========================================================================

    // TicksIncoming — 3 фиксированных exchange-тега (как в GetExchangeTag).
    private static readonly CounterBatcher TicksIncomingBinance = new(
        TicksIncoming, new KeyValuePair<string, object?>[] { new("exchange", "binance") });
    private static readonly CounterBatcher TicksIncomingKraken = new(
        TicksIncoming, new KeyValuePair<string, object?>[] { new("exchange", "kraken") });
    private static readonly CounterBatcher TicksIncomingOther = new(
        TicksIncoming, new KeyValuePair<string, object?>[] { new("exchange", "unknown") });

    // WsMessagesReceived — по комбинации exchange+symbol, создаётся лениво при первом сообщении.
    private static readonly ConcurrentDictionary<(string Exchange, string Symbol), CounterBatcher> WsMessagesBatchers =
        new();

    /// <summary>
    /// Инкремент <c>ticks.incoming</c> в hot path. Маппит exchange в фиксированный батчер.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void IncrementTicksIncoming(string exchange)
        => BatcherForExchange(exchange, TicksIncomingBinance, TicksIncomingKraken, TicksIncomingOther).Add();

    /// <summary>
    /// Инкремент <c>ticks.dropped</c> в hot path. Без аллокаций и lock: маппит exchange
    /// в фиксированный Interlocked-аккумулятор, который читает ObservableGauge TicksDropped.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void IncrementTicksDropped(string exchange)
    {
        switch (exchange)
        {
            case "Binance":
                Interlocked.Increment(ref _ticksDroppedBinance);
                break;
            case "Kraken":
                Interlocked.Increment(ref _ticksDroppedKraken);
                break;
            default:
                Interlocked.Increment(ref _ticksDroppedOther);
                break;
        }
    }

    /// <summary>
    /// Инкремент <c>ws.messages.received</c> в hot path. Получает/создаёт батчер
    /// под (exchange, symbol) без аллокаций на сам инкремент.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void IncrementWsMessagesReceived(string exchange, string symbol)
    {
        var key = (exchange, symbol);
        if (!WsMessagesBatchers.TryGetValue(key, out var batcher))
        {
            batcher = WsMessagesBatchers.GetOrAdd(key, k =>
                new CounterBatcher(WsMessagesReceived, new[]
                {
                    new KeyValuePair<string, object?>("exchange", k.Exchange),
                    new KeyValuePair<string, object?>("symbol", k.Symbol)
                }));
        }
        batcher.Add();
    }

    /// <summary>
    /// Выносит накопленные значения всех батчеров в OTel. Вызывается один раз за батч
    /// из writer loop и при финальном сбросе на остановке.
    /// </summary>
    public static void FlushMetricBatchers()
    {
        TicksIncomingBinance.Flush();
        TicksIncomingKraken.Flush();
        TicksIncomingOther.Flush();

        foreach (var batcher in WsMessagesBatchers.Values)
        {
            batcher.Flush();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CounterBatcher BatcherForExchange(
        string exchange, CounterBatcher binance, CounterBatcher kraken, CounterBatcher other)
        => exchange switch
        {
            "Binance" => binance,
            "Kraken" => kraken,
            _ => other
        };
}
