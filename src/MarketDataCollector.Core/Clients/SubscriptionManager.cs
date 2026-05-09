using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Управляет подпиской на тикеры с политикой повторных попыток на базе Polly.
/// </summary>
public class SubscriptionManager : ISubscriptionManager
{
    private readonly IWebSocketConnectionManager _connectionManager;
    private readonly WebSocketClientOptions _options;
    private readonly ILogger<SubscriptionManager> _logger;
    private readonly Func<string, CancellationToken, Task> _subscribeAction;

    public SubscriptionManager(
        IWebSocketConnectionManager connectionManager,
        IOptions<WebSocketClientOptions> options,
        ILogger<SubscriptionManager> logger,
        Func<string, CancellationToken, Task> subscribeAction)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _subscribeAction = subscribeAction ?? throw new ArgumentNullException(nameof(subscribeAction));
    }

    /// <inheritdoc />
    public async Task SubscribeWithRetryAsync(string symbol, CancellationToken cancellationToken)
    {
        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: _options.MaxSubscribeRetries,
                sleepDurationProvider: retryAttempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.LogWarning(
                        exception,
                        "Попытка подписки {RetryCount} не удалась. Повтор через {Delay}s.",
                        retryCount, timeSpan.TotalSeconds);
                });

        await retryPolicy.ExecuteAsync(async () =>
        {
            _logger.LogInformation("Подписка на тикер {Symbol}...", symbol);
            await _subscribeAction(symbol, cancellationToken);
            _logger.LogInformation("Успешно подписались на тикер {Symbol}.", symbol);
        });
    }
}
