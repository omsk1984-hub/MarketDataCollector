namespace MarketDataCollector.Core.Configuration;

/// <summary>
/// Параметры конфигурации WebSocket-клиента.
/// Привязываются к секции конфигурации через IOptions<WebSocketClientOptions>.
/// </summary>
public class WebSocketClientOptions
{
    public const string SectionName = "WebSocketClient";

    /// <summary>
    /// Начальная задержка перед первой попыткой переподключения.
    /// По умолчанию: 5 секунд.
    /// </summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Максимальная задержка между попытками переподключения (cap для экспоненциального backoff).
    /// По умолчанию: 60 секунд.
    /// </summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Максимальное количество попыток переподключения в ReceiveLoop до выхода.
    /// По умолчанию: 3.
    /// </summary>
    public int MaxInternalReconnectAttempts { get; set; } = 3;

    /// <summary>
    /// Количество попыток подписки при ошибке.
    /// По умолчанию: 3.
    /// </summary>
    public int MaxSubscribeRetries { get; set; } = 3;

    /// <summary>
    /// Размер буфера для приёма сообщений (в байтах).
    /// По умолчанию: 4096.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 4096;

    /// <summary>
    /// Таймаут ожидания при остановке клиента (Dispose).
    /// По умолчанию: 5 секунд.
    /// </summary>
    public TimeSpan DisposeTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
