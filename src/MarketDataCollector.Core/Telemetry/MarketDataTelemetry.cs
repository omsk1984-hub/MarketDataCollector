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

    /// <summary>
    /// Количество тиков, дропнутых каналом (TryWrite=false из-за DropOldest).
    /// Теги: exchange
    /// </summary>
    public static readonly Counter<long> TicksDropped = Instance.CreateCounter<long>(
        name: "ticks.dropped",
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
    /// Текущий fill level по каждому каналу (instant).
    /// Теги: channel_index
    /// </summary>
    public static readonly UpDownCounter<long> ChannelFillLevel = Instance.CreateUpDownCounter<long>(
        name: "processor.channel.fill_level",
        unit: "count",
        description: "Current ticks in channel by channel_index");

    /// <summary>
    /// Оценка дропнутых тиков через DropOldest (cumulative).
    /// </summary>
    public static readonly Counter<long> TicksDroppedSilently = Instance.CreateCounter<long>(
        name: "ticks.dropped.silently",
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

    // TicksDropped — те же 3 exchange-тега.
    private static readonly CounterBatcher TicksDroppedBinance = new(
        TicksDropped, new KeyValuePair<string, object?>[] { new("exchange", "binance") });
    private static readonly CounterBatcher TicksDroppedKraken = new(
        TicksDropped, new KeyValuePair<string, object?>[] { new("exchange", "kraken") });
    private static readonly CounterBatcher TicksDroppedOther = new(
        TicksDropped, new KeyValuePair<string, object?>[] { new("exchange", "unknown") });

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
    /// Инкремент <c>ticks.dropped</c> в hot path. Маппит exchange в фиксированный батчер.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void IncrementTicksDropped(string exchange)
        => BatcherForExchange(exchange, TicksDroppedBinance, TicksDroppedKraken, TicksDroppedOther).Add();

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

        TicksDroppedBinance.Flush();
        TicksDroppedKraken.Flush();
        TicksDroppedOther.Flush();

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
