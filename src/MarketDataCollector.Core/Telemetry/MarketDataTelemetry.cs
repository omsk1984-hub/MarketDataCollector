using System.Diagnostics;
using System.Diagnostics.Metrics;

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
}
