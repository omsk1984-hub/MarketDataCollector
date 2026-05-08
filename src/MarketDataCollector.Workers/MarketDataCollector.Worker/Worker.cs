using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace MarketDataCollector.Worker;

public class Worker : BackgroundService
{
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
        _logger.LogInformation("Worker starting...");
        await RunWithRecoveryAsync(stoppingToken);
        _logger.LogInformation("Worker stopped.");
    }

    /// <summary>
    /// Запускает клиентов и процессор. При критической ошибке Worker завершается —
    /// перезапуск управляется внешним оркестратором (Docker/K8s).
    /// </summary>
    private async Task RunWithRecoveryAsync(CancellationToken stoppingToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var clientFactory = scope.ServiceProvider.GetRequiredService<IWebSocketClientFactory>();
        var marketDataProcessor = scope.ServiceProvider.GetRequiredService<IMarketDataProcessor>();

        var clients = clientFactory.CreateAllClients().ToList();

        if (clients.Count == 0)
        {
            _logger.LogError("No exchanges configured in 'Exchanges' section. Worker will exit.");
            return;
        }

        try
        {
            _logger.LogInformation("Starting {Count} WebSocket clients...", clients.Count);

            // Запускаем каждого клиента — каждый живёт своей жизнью с автовосстановлением
            foreach (var client in clients)
            {
                await client.StartAsync(stoppingToken);
            }

            marketDataProcessor.StartProcessing();
            _logger.LogInformation("Market data processor started");

            // Активный health-check: мониторинг + перезапуск отключённых клиентов
            await RunHealthCheckAsync(clients, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Worker is stopping due to cancellation.");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Critical error (processor or infrastructure). Worker will exit.");
            throw;
        }
        finally
        {
            await CleanupAsync(marketDataProcessor, clients, stoppingToken);
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

    /// <summary>
    /// Активный health-check: блокируется до отмены, периодически проверяет состояние клиентов.
    /// Если клиент отключён — запускает повторно (StartAsync идемпотентен).
    /// </summary>
    private async Task RunHealthCheckAsync(
        List<IExchangeWebSocketClient> clients,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(HealthCheckInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var connected = clients.Count(c => c.IsConnected);
            var disconnected = clients.Count - connected;

            _logger.LogInformation("Health-check: {Connected} connected, {Disconnected} disconnected",
                connected, disconnected);

            foreach (var client in clients.Where(c => !c.IsConnected))
            {
                _logger.LogWarning("Client {Exchange} ({Symbol}) is disconnected, triggering restart...",
                    client.ExchangeName, client.Symbol);

                try
                {
                    // StartAsync идемпотентен — безопасно вызывать повторно
                    await client.StartAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to restart {Exchange}", client.ExchangeName);
                }
            }
        }
    }
}
