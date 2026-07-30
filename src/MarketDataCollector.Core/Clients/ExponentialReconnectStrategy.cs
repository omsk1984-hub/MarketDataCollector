using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Стратегия переподключения с экспоненциальным backoff, jitter и ограничением попыток.
/// Jitter предотвращает thundering herd при массовом переподключении клиентов.
/// </summary>
public class ExponentialReconnectStrategy : IReconnectStrategy
{
    private readonly WebSocketClientOptions _options;
    private readonly ILogger<ExponentialReconnectStrategy> _logger;
    private readonly Random _random = new();

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

        // Добавляем jitter: ±JitterFactor от задержки.
        // Это гарантирует, что при N клиентах задержки будут разбросаны,
        // а не синхронизированы (thundering herd).
        var jitterRange = delaySeconds * _options.JitterFactor;
        var jitter = _random.NextDouble() * 2 * jitterRange - jitterRange; // [-jitterRange, +jitterRange]
        delaySeconds = Math.Max(0, delaySeconds + jitter);

        return TimeSpan.FromSeconds(delaySeconds);
    }

    /// <inheritdoc />
    public bool ShouldRetry(int attempt)
    {
        // MaxReconnectAttempts = 0 → бесконечно (пока не отменён CancellationToken)
        if (_options.MaxReconnectAttempts <= 0)
            return true;

        return attempt <= _options.MaxReconnectAttempts;
    }

    /// <inheritdoc />
    public void Reset()
    {
        _logger.LogDebug("Сброс состояния стратегии переподключения.");
    }
}
