using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace MarketDataCollector.Worker;

public class Worker : BackgroundService
{
    private static readonly TimeSpan HealthCheckInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ErrorRetryDelay = TimeSpan.FromSeconds(30);

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
        _logger.LogInformation("Worker starting...");
        await RunWithRecoveryAsync(stoppingToken);
        _logger.LogInformation("Worker stopped.");
    }

    /// <summary>
    /// Запускает клиентов и процессор с автоматическим перезапуском при ошибках.
    /// Циклически перезапускает при сбоях, пока не будет запрошена отмена.
    /// </summary>
    private async Task RunWithRecoveryAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _scopeFactory.CreateScope();
            var clientFactory = scope.ServiceProvider.GetRequiredService<IWebSocketClientFactory>();
            var marketDataProcessor = scope.ServiceProvider.GetRequiredService<IMarketDataProcessor>();

            var clients = clientFactory.CreateAllClients().ToList();

            if (clients.Count == 0)
            {
                _logger.LogError("No exchanges configured in 'Exchanges' section. Retrying in {Delay}s...",
                    ErrorRetryDelay.TotalSeconds);
                await Task.Delay(ErrorRetryDelay, stoppingToken);
                continue;
            }

            try
            {
                _logger.LogInformation("Starting {Count} WebSocket clients...", clients.Count);

                var startTasks = clients.Select(client => client.StartAsync(stoppingToken));
                await Task.WhenAll(startTasks);

                marketDataProcessor.StartProcessing();
                _logger.LogInformation("Market data processor started");

                // Health-check таймер
                using var healthCheckTimer = new Timer(
                    _ => LogClientStatus(clients),
                    null,
                    HealthCheckInterval,
                    HealthCheckInterval);

                // Блокируем до отмены
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Worker is stopping due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred. Retrying in {Delay}s...",
                    ErrorRetryDelay.TotalSeconds);
            }
            finally
            {
                await CleanupAsync(marketDataProcessor, clients, stoppingToken);
            }

            // Задержка перед перезапуском (если не запрошена отмена)
            if (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(ErrorRetryDelay, stoppingToken);
            }
        }
    }

    private async Task CleanupAsync(
        IMarketDataProcessor marketDataProcessor,
        List<IExchangeWebSocketClient> clients,
        CancellationToken stoppingToken)
    {
        try
        {
            await marketDataProcessor.StopProcessingAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping market data processor");
        }

        _logger.LogInformation("Stopping WebSocket clients...");
        var stopTasks = clients.Select(client => StopClientAsync(client));
        await Task.WhenAll(stopTasks);
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
}
