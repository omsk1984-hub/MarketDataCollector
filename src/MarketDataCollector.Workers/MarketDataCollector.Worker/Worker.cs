using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace MarketDataCollector.Worker;

public class Worker : BackgroundService
{
    private const int MaxSubscribeRetryAttempts = 5;
    private static readonly TimeSpan InitialSubscribeDelay = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan HealthCheckInterval = TimeSpan.FromSeconds(30);

    private readonly ILogger<Worker> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

    public Worker(
        ILogger<Worker> logger,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker starting with automatic recovery mode...");

        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _scopeFactory.CreateScope();
            var clientFactory = scope.ServiceProvider.GetRequiredService<IWebSocketClientFactory>();
            var marketDataProcessor = scope.ServiceProvider.GetRequiredService<IMarketDataProcessor>();

            var clients = clientFactory.CreateAllClients().ToList();

            if (clients.Count == 0)
            {
                _logger.LogError("No exchanges configured in 'Exchanges' section.");
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                continue;
            }

            try
            {
                _logger.LogInformation("Starting {Count} WebSocket clients with automatic recovery...", clients.Count);

                // Запускаем всех клиентов в режиме автоматического восстановления
                var startTasks = clients.Select(client => StartClientWithRecoveryAsync(client, stoppingToken));
                await Task.WhenAll(startTasks);

                // Запускаем обработчик данных
                marketDataProcessor.StartProcessing();
                _logger.LogInformation("Market data processor started");

                // Упрощённый health-check: просто мониторим состояние
                using var healthCheckTimer = new Timer(
                    _ => LogClientStatus(clients),
                    null,
                    HealthCheckInterval,
                    HealthCheckInterval);

                // Ожидаем отмены
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Worker is stopping due to cancellation.");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred in the worker. Retrying in 30 seconds...");
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
            finally
            {
                // Останавливаем обработчик
                await marketDataProcessor.StopProcessingAsync(stoppingToken);

                // Останавливаем всех клиентов
                _logger.LogInformation("Stopping WebSocket clients...");
                var stopTasks = clients.Select(client => StopClientAsync(client));
                await Task.WhenAll(stopTasks);
            }
        }

        _logger.LogInformation("Worker stopped.");
    }

    private async Task StartClientWithRecoveryAsync(IExchangeWebSocketClient client, CancellationToken stoppingToken)
    {
        try
        {
            await client.StartAsync(stoppingToken);
            _logger.LogInformation("Started automatic recovery for {Exchange} ({Symbol})",
                client.ExchangeName, client.Symbol);

            // Подписываемся на тикер
            await SubscribeWithRetryAsync(client, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start client {Exchange} ({Symbol})",
                client.ExchangeName, client.Symbol);
        }
    }

    private async Task StopClientAsync(IExchangeWebSocketClient client)
    {
        try
        {
            await client.StopAsync(CancellationToken.None);
            _logger.LogInformation("Stopped {Exchange}", client.ExchangeName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while stopping {Exchange}", client.ExchangeName);
        }
    }

    private void LogClientStatus(List<IExchangeWebSocketClient> clients)
    {
        var connected = clients.Count(c => c.IsConnected);
        var disconnected = clients.Count - connected;

        _logger.LogInformation("Health-check: {Connected} connected, {Disconnected} disconnected",
            connected, disconnected);

        foreach (var client in clients.Where(c => !c.IsConnected))
        {
            _logger.LogWarning("Client {Exchange} ({Symbol}) is disconnected",
                client.ExchangeName, client.Symbol);
        }
    }

    /// <summary>
    /// Подписка на тикер с экспоненциальной задержкой при ошибке.
    /// </summary>
    private async Task SubscribeWithRetryAsync(
        IExchangeWebSocketClient client,
        CancellationToken stoppingToken)
    {
        var delay = InitialSubscribeDelay;

        for (int attempt = 1; attempt <= MaxSubscribeRetryAttempts; attempt++)
        {
            try
            {
                await client.SubscribeToTicker(client.Symbol, stoppingToken);
                _logger.LogInformation(
                    "Subscribed to ticker {Symbol} on {Exchange} (attempt {Attempt}/{MaxAttempts})",
                    client.Symbol, client.ExchangeName, attempt, MaxSubscribeRetryAttempts);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Subscribe attempt {Attempt}/{MaxAttempts} failed for {Symbol} on {Exchange}. " +
                    "Retrying in {Delay}s...",
                    attempt, MaxSubscribeRetryAttempts, client.Symbol, client.ExchangeName, delay.TotalSeconds);

                if (attempt < MaxSubscribeRetryAttempts)
                {
                    await Task.Delay(delay, stoppingToken);
                    delay = delay * 2;
                }
            }
        }

        _logger.LogError(
            "All {MaxAttempts} subscribe attempts exhausted for {Symbol} on {Exchange}.",
            MaxSubscribeRetryAttempts, client.Symbol, client.ExchangeName);
    }
}
