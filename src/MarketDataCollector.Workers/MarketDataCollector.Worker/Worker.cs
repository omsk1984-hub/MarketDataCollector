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
        _logger.LogInformation("Worker starting...");

        // Список клиентов, которые были успешно подключены — для health-check
        List<IExchangeWebSocketClient> connectedClients = new();

        while (!stoppingToken.IsCancellationRequested)
        {
            // Create a scope for each attempt (scoped services)
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
                // Шаг 1: Подключаем каждого клиента независимо, ошибки изолируются
                connectedClients = new List<IExchangeWebSocketClient>();
                foreach (var client in clients)
                {
                    try
                    {
                        await client.ConnectAsync(stoppingToken);
                        connectedClients.Add(client);
                        _logger.LogInformation("Connected to {Exchange} ({Symbol})", client.ExchangeName, client.Symbol);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to connect to {Exchange} ({Symbol}). " +
                            "Will be retried by health-check.", client.ExchangeName, client.Symbol);
                    }
                }

                if (connectedClients.Count == 0)
                {
                    _logger.LogError("No clients connected. Retrying in 30 seconds...");
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                    continue;
                }

                _logger.LogInformation("Connected to {Connected}/{Total} WebSocket(s)", connectedClients.Count, clients.Count);

                // Шаг 2: Подписываемся на тикеры независимо для каждого клиента
                foreach (var client in connectedClients)
                {
                    try
                    {
                        await SubscribeWithRetryAsync(client, stoppingToken);
                        _logger.LogInformation("Subscribed {Exchange} to {Symbol}", client.ExchangeName, client.Symbol);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        // Ошибка подписки — не критична, клиент остаётся подключённым
                        _logger.LogError(ex, "Failed to subscribe {Exchange} to {Symbol}. " +
                            "Will be retried by health-check.", client.ExchangeName, client.Symbol);
                    }
                }

                // Запускаем обработчик очереди
                marketDataProcessor.StartProcessing();
                _logger.LogInformation("Market data processor started");

                // Шаг 3: Запускаем health-check таймер и ожидаем отмены
                _logger.LogInformation("Worker is running. Health-check interval: {Interval}s", HealthCheckInterval.TotalSeconds);

                using var healthCheckTimer = new Timer(
                    async _ => await RestartDisconnectedClientsAsync(connectedClients, stoppingToken),
                    null,
                    HealthCheckInterval,
                    HealthCheckInterval);

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
                // Останавливаем health-check таймер (через using выше)

                // Останавливаем обработчик (graceful shutdown)
                await marketDataProcessor.StopProcessingAsync(stoppingToken);

                // Корректно отключаем все клиенты
                foreach (var client in connectedClients)
                {
                    if (client.IsConnected)
                    {
                        try
                        {
                            await client.DisconnectAsync(CancellationToken.None);
                            _logger.LogInformation("Disconnected {Exchange}", client.ExchangeName);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error while disconnecting {Exchange}", client.ExchangeName);
                        }
                    }
                }
            }
        }

        _logger.LogInformation("Worker stopped.");
    }

    /// <summary>
    /// Health-check: перезапускает клиентов, которые отключились.
    /// Вызывается таймером каждые 30 секунд.
    /// </summary>
    private async Task RestartDisconnectedClientsAsync(
        List<IExchangeWebSocketClient> clients,
        CancellationToken stoppingToken)
    {
        foreach (var client in clients)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            if (!client.IsConnected)
            {
                _logger.LogWarning("Health-check: {Exchange} ({Symbol}) is disconnected. Attempting restart...",
                    client.ExchangeName, client.Symbol);
                try
                {
                    await client.ConnectAsync(stoppingToken);
                    await SubscribeWithRetryAsync(client, stoppingToken);
                    _logger.LogInformation("Health-check: {Exchange} restarted successfully", client.ExchangeName);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Health-check: Failed to restart {Exchange}", client.ExchangeName);
                }
            }
        }
    }

    /// <summary>
    /// Подписка на тикер с экспоненциальной задержкой при ошибке.
    /// После исчерпания попыток выбрасывает исключение — вызывающий код решает, что делать.
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
                // Отмена — пробрасываем наверх, не маскируем
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
                    delay = delay * 2; // Экспоненциальная задержка: 2с, 4с, 8с, 16с, 32с
                }
            }
        }

        _logger.LogError(
            "All {MaxAttempts} subscribe attempts exhausted for {Symbol} on {Exchange}. " +
            "Client will remain without subscription until health-check restarts it.",
            MaxSubscribeRetryAttempts, client.Symbol, client.ExchangeName);

        // Пробрасываем исключение — вызывающий код (Worker) поймает и продолжит
        throw new InvalidOperationException(
            $"Subscribe exhausted after {MaxSubscribeRetryAttempts} attempts for {client.ExchangeName}/{client.Symbol}");
    }
}
