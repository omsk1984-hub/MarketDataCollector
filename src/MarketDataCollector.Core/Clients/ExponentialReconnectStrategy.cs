using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Стратегия переподключения с экспоненциальной задержкой и ограничением сверху (cap).
/// </summary>
public class ExponentialReconnectStrategy : IReconnectStrategy
{
    private readonly WebSocketClientOptions _options;
    private readonly ILogger<ExponentialReconnectStrategy> _logger;

    public ExponentialReconnectStrategy(
        IOptions<WebSocketClientOptions> options,
        ILogger<ExponentialReconnectStrategy> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public TimeSpan GetDelay(int attempt)
    {
        if (attempt <= 0)
            throw new ArgumentOutOfRangeException(nameof(attempt), "Номер попытки должен быть >= 1.");

        // Экспоненциальный backoff с cap: delay * 2^(attempt-1), но не больше MaxReconnectDelay
        var delaySeconds = Math.Min(
            _options.ReconnectDelay.TotalSeconds * Math.Pow(2, attempt - 1),
            _options.MaxReconnectDelay.TotalSeconds);

        return TimeSpan.FromSeconds(delaySeconds);
    }

    /// <inheritdoc />
    public bool ShouldRetry(int attempt)
    {
        // В фоновом цикле восстановления пытаемся бесконечно (пока не отменён CancellationToken).
        // Внутренние лимиты обрабатываются вызывающим кодом.
        return true;
    }

    /// <inheritdoc />
    public void Reset()
    {
        _logger.LogDebug("Сброс состояния стратегии переподключения.");
    }
}
