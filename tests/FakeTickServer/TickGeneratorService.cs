using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace FakeTickServer;

/// <summary>
/// BackgroundService, который генерирует случайные тики в формате Binance trade stream
/// и отправляет их всем подключённым WebSocket-клиентам.
/// </summary>
public class TickGeneratorService : BackgroundService
{
    private readonly Settings _settings;
    private readonly ILogger<TickGeneratorService> _logger;
    private readonly ConcurrentDictionary<string, ClientState> _clients = new();
    private readonly Random _random = new();

    /// <summary>Глобальный счётчик trade ID.</summary>
    private long _globalTradeId;

    /// <summary>Счётчик отправленных сообщений для RPS-мониторинга.</summary>
    private long _sentCount;

    /// <summary>Таймер для логирования статистики RPS.</summary>
    private DateTime _lastStatsTime = DateTime.UtcNow;
    private long _lastSentCount;

    /// <summary>
    /// Конструктор.
    /// </summary>
    public TickGeneratorService(Settings settings, ILogger<TickGeneratorService> logger)
    {
        _settings = settings;
        _logger = logger;

        // Стартуем trade ID от случайного значения, чтобы было похоже на реальные данные
        _globalTradeId = _random.NextInt64(100_000_000, 1_000_000_000);
    }

    /// <summary>
    /// Добавляет WebSocket-клиента в список получателей тиков.
    /// </summary>
    public void AddClient(WebSocket webSocket, string symbol)
    {
        var clientId = Guid.NewGuid().ToString("N")[..8];
        _clients.TryAdd(clientId, new ClientState(webSocket, symbol));
        _logger.LogInformation("Клиент {ClientId} подключился. Всего клиентов: {Count}, symbol: {Symbol}",
            clientId, _clients.Count, symbol);
    }

    /// <summary>
    /// Удаляет WebSocket-клиента из списка получателей.
    /// </summary>
    public void RemoveClient(string clientId)
    {
        if (_clients.TryRemove(clientId, out _))
        {
            _logger.LogInformation("Клиент {ClientId} отключился. Всего клиентов: {Count}",
                clientId, _clients.Count);
        }
    }

    /// <summary>
    /// Возвращает количество подключённых клиентов.
    /// </summary>
    public int ClientCount => _clients.Count;

    /// <summary>
    /// Возвращает текущий RPS (отправлено сообщений за последнюю секунду).
    /// </summary>
    public double GetCurrentRps()
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - _lastStatsTime).TotalSeconds;
        if (elapsed < 0.1) return 0;

        var sent = Interlocked.Read(ref _sentCount);
        var delta = sent - _lastSentCount;
        _lastSentCount = sent;
        _lastStatsTime = now;
        return delta / elapsed;
    }

    /// <summary>
    /// Основной цикл генерации тиков.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("TickGenerator запущен. Целевой RPS: {Rps}, символов: {Symbols}",
            _settings.Rps, string.Join(", ", _settings.Symbols));

        _logger.LogDebug("Используется Stopwatch-контроль RPS для точного соблюдения целевого RPS");

        // Таймер для логирования статистики раз в 5 секунд
        using var statsTimer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var statsTask = LogStatsPeriodicallyAsync(statsTimer, stoppingToken);

        // Счётчик ожидаемого количества тиков по таймеру
        long expectedTotal = 0;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (_clients.IsEmpty)
                {
                    // Если нет клиентов — сбрасываем expectedTotal и ждём
                    expectedTotal = (long)(stopwatch.Elapsed.TotalMilliseconds * _settings.Rps / 1000.0);
                    await Task.Delay(100, stoppingToken);
                    continue;
                }

                // Вычисляем, сколько тиков должно было быть отправлено с момента старта
                var elapsedMs = stopwatch.Elapsed.TotalMilliseconds;
                var newExpected = (long)(elapsedMs * _settings.Rps / 1000.0);
                var need = (int)(newExpected - expectedTotal);

                if (need > 0)
                {
                    expectedTotal = newExpected;

                    // Равномерно распределяем need тиков между клиентами
                    var totalClients = _clients.Count;
                    var perClient = need / totalClients;
                    var extra = need % totalClients;

                    foreach (var (clientId, clientState) in _clients)
                    {
                        if (stoppingToken.IsCancellationRequested) break;

                        var count = perClient + (extra > 0 ? 1 : 0);
                        if (extra > 0) extra--;

                        for (int j = 0; j < count; j++)
                        {
                            try
                            {
                                var tickJson = GenerateTick(clientState.Symbol);
                                var bytes = Encoding.UTF8.GetBytes(tickJson);
                                var segment = new ArraySegment<byte>(bytes);

                                await clientState.WebSocket.SendAsync(
                                    segment, WebSocketMessageType.Text, true, stoppingToken);

                                Interlocked.Increment(ref _sentCount);
                            }
                            catch (WebSocketException ex) when (
                                ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely ||
                                ex.WebSocketErrorCode == WebSocketError.InvalidState)
                            {
                                _logger.LogWarning("Клиент {ClientId} отключился (WebSocket ошибка): {Error}",
                                    clientId, ex.Message);
                                RemoveClient(clientId);
                                break; // прерываем inner loop для этого клиента
                            }
                            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                            {
                                break;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex, "Ошибка при отправке клиенту {ClientId}", clientId);
                                RemoveClient(clientId);
                                break; // прерываем inner loop для этого клиента
                            }
                        }
                    }
                }
                else
                {
                    // Ничего отправлять не нужно — спим 10 мс, чтобы не нагружать CPU
                    await Task.Delay(10, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемое завершение
        }

        _logger.LogInformation("TickGenerator остановлен");
    }

    /// <summary>
    /// Генерирует JSON-тик в формате Binance trade stream.
    /// Совместимость с BinanceWebSocketClient.ProcessMessageAsync.
    /// </summary>
    private string GenerateTick(string symbol)
    {
        var tradeId = Interlocked.Increment(ref _globalTradeId);
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Случайное отклонение цены +/- 0.2%
        var priceVariation = 1.0 + (_random.NextDouble() - 0.5) * 0.004;
        var price = _settings.BasePrice * (decimal)priceVariation;

        // Случайный объём: 0.0001 – 0.1 (для BTC) или 0.1 – 10 (для альткоинов)
        var isBtc = symbol.StartsWith("btc", StringComparison.OrdinalIgnoreCase);
        var volume = isBtc
            ? 0.0001m + (decimal)_random.NextDouble() * 0.0999m
            : 0.1m + (decimal)_random.NextDouble() * 9.9m;

        var isBuyerMaker = _random.Next(2) == 0;
        var isBestMatch = _random.Next(2) == 0;

        // Сериализуем в JSON (используем System.Text.Json)
        return JsonSerializer.Serialize(new BinanceTick
        {
            e = "trade",
            E = unixTimeMs,
            s = symbol.ToUpperInvariant(),
            t = tradeId,
            p = price,
            q = volume,
            T = unixTimeMs,
            m = isBuyerMaker,
            M = isBestMatch
        });
    }

    /// <summary>
    /// Периодическое логирование статистики RPS.
    /// </summary>
    private async Task LogStatsPeriodicallyAsync(PeriodicTimer timer, CancellationToken ct)
    {
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                var actualRps = GetCurrentRps();
                var targetRps = _settings.Rps;
                var clients = _clients.Count;
                _logger.LogInformation(
                    "Статус: клиентов={Clients}, targetRps={TargetRps}, actualRps={ActualRps:F0}",
                    clients, targetRps, actualRps);
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо
        }
    }

    /// <summary>
    /// Состояние подключённого WebSocket-клиента.
    /// </summary>
    private record ClientState(WebSocket WebSocket, string Symbol);

    /// <summary>
    /// DTO для сериализации в Binance trade JSON.
    /// </summary>
    private record BinanceTick
    {
        // ReSharper disable InconsistentNaming
        // Имена полей — ровно как в Binance API (JSON → snake_case)
        public string e { get; init; } = "trade";
        public long E { get; init; }
        public string s { get; init; } = "";
        public long t { get; init; }
        public decimal p { get; init; }
        public decimal q { get; init; }
        public long T { get; init; }
        public bool m { get; init; }
        public bool M { get; init; }
    }
}
