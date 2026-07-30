using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Core.Telemetry;

namespace MarketDataCollector.Worker;

public class Worker : BackgroundService
{
    private static readonly TimeSpan HealthCheckInterval = TimeSpan.FromSeconds(10);

    private readonly ILogger<Worker> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

    // Предыдущие значения для расчёта дельт OpenTelemetry метрик
    private int _lastEstimatedDropped;
    private long _lastBacklog;

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

        try
        {
            // ВАЖНО: Сначала запускаем процессор и агрегатор, чтобы их Channel'ы
            // были готовы к приёму данных ДО того, как WebSocket-клиенты начнут
            // отправлять тики. Это предотвращает потерю данных в "канале-призраке"
            // (старый channel, созданный в конструкторе, заменяется при старте).

            // Запускаем процессор — возвращает фоновую задачу consumer'ов.
            // Не await'им здесь — она работает параллельно с health-check loop.
            // Фатальные ошибки fault'ят task (IsFaulted = true).
            var processorTask = marketDataProcessor.StartProcessingAsync(stoppingToken);

            // Запускаем агрегатор свечей
            var aggregatorTask = tickAggregator.StartAsync(stoppingToken);

            // Теперь запускаем WebSocket-клиентов — staggered startup с интервалом 2с
            // чтобы предотвратить стартовый пик backlog (36,243) и дропы (33,456).
            // Каждый клиент начинает с ~6,400 msg/s, stagger даёт consumer'у фору.
            _logger.LogInformation("Starting {Count} WebSocket clients with staggered startup (2s interval)...", clients.Count);
            for (int i = 0; i < clients.Count; i++)
            {
                _ = clients[i].StartAsync(stoppingToken); // fire-and-forget — фоновая задача recovery loop
                if (i < clients.Count - 1)
                    await Task.Delay(2000, stoppingToken); // 2s stagger между стартами
            }

            // Активный health-check: мониторинг + перезапуск отключённых клиентов
            await RunHealthCheckAsync(clients, marketDataProcessor, stoppingToken);

            // Наблюдаем за фоновой задачей процессора — фатальные ошибки fault'ят task
            if (processorTask.IsFaulted)
            {
                throw new InvalidOperationException(
                    "MarketDataProcessor background task failed",
                    processorTask.Exception?.InnerException);
            }

            // Наблюдаем за фоновой задачей агрегатора
            if (aggregatorTask.IsFaulted)
            {
                throw new InvalidOperationException(
                    "TickAggregator background task failed",
                    aggregatorTask.Exception?.InnerException);
            }
        }
        catch (OperationCanceledException)
        {
            var remaining = marketDataProcessor.GetChannelCount();
            _logger.LogWarning("Worker is stopping due to cancellation. Remaining ticks in channel: {Remaining}", remaining);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Critical error (processor or infrastructure). Worker will exit.");
            throw;
        }
        finally
        {
            // Используем CancellationToken.None, т.к. stoppingToken уже отменён.
            // Добавляем safety-net timeout 30 секунд, чтобы не зависнуть навсегда.
            using var cleanupCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await CleanupAsync(marketDataProcessor, tickAggregator, clients, cleanupCts.Token);
        }
    }

    private async Task CleanupAsync(
        IMarketDataProcessor marketDataProcessor,
        ITickAggregator tickAggregator,
        List<IExchangeWebSocketClient> clients,
        CancellationToken stoppingToken)
    {
        // ВАЖНО: Сначала останавливаем WebSocket-клиенты, чтобы они прекратили
        // отправку данных в MarketDataProcessor и TickAggregator.
        // Если остановить процессор первым, клиенты будут пытаться писать в закрытый Channel,
        // что вызовет ChannelClosedException.
        _logger.LogInformation("Stopping WebSocket clients...");
        var stopTasks = clients.Select(client => StopClientAsync(client));
        await Task.WhenAll(stopTasks);

        try
        {
            await tickAggregator.StopAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping tick aggregator");
        }

        // Останавливаем процессор — он дочитает остатки из канала и выполнит финальный flush
        try
        {
            await marketDataProcessor.StopProcessingAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error stopping market data processor");
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
            var incomingRps = clients.Sum(c => c.GetMessagesPerSecond());
            var processedRps = marketDataProcessor.GetProcessedRps();

            // Per-channel fill percentages
            var fillLevels = marketDataProcessor.GetChannelFillLevels();
            var fillPercents = string.Join(", ", fillLevels.Select(f =>
                f.Capacity > 0 ? $"{(double)f.Count / f.Capacity * 100.0:F1}%" : "0%"));

            // Total fill (sum over all channels)
            var totalCount = fillLevels.Sum(f => f.Count);
            var totalCapacity = fillLevels.Sum(f => f.Capacity);
            var totalFillPercent = totalCapacity > 0
                ? (double)totalCount / totalCapacity * 100.0
                : 0.0;

            // Estimated dropped (via backlog, because DropOldest never returns TryWrite=false)
            var estimatedDropped = marketDataProcessor.GetEstimatedDroppedCount();
            var totalChannelIncoming = marketDataProcessor.GetTotalIncomingCount();
            var totalChannelReceived = marketDataProcessor.GetTotalReceivedCount();

            // OpenTelemetry: update metrics (UpDownCounter использует Add(delta),
            // Counter.Add(delta) — для cumulative метрик)
            var currentBacklog = totalChannelIncoming - totalChannelReceived;
            var backlogDelta = currentBacklog - _lastBacklog;
            if (backlogDelta != 0)
            {
                MarketDataTelemetry.ChannelBacklog.Add(backlogDelta);
                _lastBacklog = currentBacklog;
            }

            var droppedDelta = estimatedDropped - _lastEstimatedDropped;
            if (droppedDelta > 0)
            {
                MarketDataTelemetry.TicksDroppedSilently.Add(droppedDelta);
                _lastEstimatedDropped = estimatedDropped;
            }

            // Per-channel fill через уже существующую гистограмму ChannelFill,
            // которая записывается в ProcessBatchesAsync раз в 10 сек.
            // Здесь не дублируем — гистограмма точнее для распределения.

            // Compact health-check log
            _logger.LogInformation(
                "Health-check: {Connected} connected, {Disconnected} disconnected | " +
                "fills: {FillPercents} | total: {TotalFill:F1}% | dropped: ~{Dropped} | " +
                "RPS: Incoming={IncomingRps:F1} msg/s, Processed={ProcessedRps:F1} ticks/s",
                connected, disconnected, fillPercents, totalFillPercent, estimatedDropped,
                incomingRps, processedRps);

            // Warning if significant drops detected
            if (estimatedDropped > 100)
            {
                _logger.LogWarning(
                    "Health-check: Дропнуто ~{Dropped} тиков из-за переполнения канала (DropOldest). " +
                    "incoming={In}, received={Rec}. Увеличьте ChannelCapacity или оптимизируйте скорость записи.",
                    estimatedDropped, totalChannelIncoming, totalChannelReceived);
            }

            // Recovery-loop клиента уже обрабатывает переподключение.
            // Health-check только логирует дисконнект для операционной видимости.
            foreach (var client in clients.Where(c => !c.IsConnected))
            {
                _logger.LogWarning(
                    "Client {Exchange} ({Symbol}) is disconnected. Recovery-loop should handle reconnection.",
                    client.ExchangeName, client.Symbol);
            }
        }
    }
}
