using MarketDataCollector.Core.Interfaces;

namespace MarketDataCollector.Worker;

public class Worker : BackgroundService
{
    private static readonly TimeSpan HealthCheckInterval = TimeSpan.FromSeconds(10);

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
        var tickAggregator = scope.ServiceProvider.GetRequiredService<ITickAggregator>();

        var clients = clientFactory.CreateAllClients().ToList();

        if (clients.Count == 0)
        {
            _logger.LogError("No exchanges configured in 'Exchanges' section. Worker will exit.");
            return;
        }

        // Используем отдельный CancellationTokenSource для прерывания health-check
        // при критической ошибке процессора
        using var processorErrorCts = new CancellationTokenSource();
        var processorErrorException = (Exception?)null;

        try
        {
            _logger.LogInformation("Starting {Count} WebSocket clients...", clients.Count);

            // Подписываемся на критические ошибки процессора
            marketDataProcessor.OnError += (sender, ex) =>
            {
                _logger.LogCritical(ex, "MarketDataProcessor raised a critical error. Worker will exit.");
                processorErrorException = ex;
                processorErrorCts.Cancel();
            };

            // Запускаем каждого клиента — каждый живёт своей жизнью с автовосстановлением
            var tasks = clients.Select(client => client.StartAsync(stoppingToken));
            await Task.WhenAll(tasks);

            // Запускаем процессор в фоновом режиме — он работает до отмены stoppingToken
            _ = marketDataProcessor.StartProcessingAsync(stoppingToken);

            // Запускаем агрегатор свечей
            await tickAggregator.StartAsync(stoppingToken);

            // Активный health-check: мониторинг + перезапуск отключённых клиентов
            // Используем объединённый токен — отмена либо от stoppingToken, либо от ошибки процессора
            using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, processorErrorCts.Token);
            await RunHealthCheckAsync(clients, marketDataProcessor, combinedCts.Token);

            // Выбрасываем ошибку для внешнего оркестратора
            if (processorErrorException != null)
            {
                throw new InvalidOperationException("MarketDataProcessor failed", processorErrorException);
            }
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
            await CleanupAsync(marketDataProcessor, tickAggregator, clients, stoppingToken);
        }
    }

    private async Task CleanupAsync(
        IMarketDataProcessor marketDataProcessor,
        ITickAggregator tickAggregator,
        List<IExchangeWebSocketClient> clients,
        CancellationToken stoppingToken)
    {
        try
        {
            await tickAggregator.StopAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping tick aggregator");
        }

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
        IMarketDataProcessor marketDataProcessor,
        CancellationToken stoppingToken)
    {
        _logger.LogInformation("Health-check: Запущен");
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

            // RPS метрики
            var incommingRps = clients.Sum(c => c.GetMessagesPerSecond());
            var processedRps = marketDataProcessor.GetProcessedRps();

            // Детальные RPS по каждому клиенту
            var clientDetails = string.Join(", ", clients.Select(c =>
                $"{c.ExchangeName}_{c.Symbol}={c.GetMessagesPerSecond():F1}"));

            _logger.LogInformation(
                "Health-check: {Connected} connected, {Disconnected} disconnected | " +
                "RPS: Incoming={IncomingRps:F1} msg/s, Processed={ProcessedRps:F1} ticks/s | Clients: {ClientDetails}",
                connected, disconnected, incommingRps, processedRps, clientDetails);

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
