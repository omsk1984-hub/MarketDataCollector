using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace FakeTickServer;

/// <summary>
/// BackgroundService, который генерирует случайные тики в формате Binance trade stream
/// и отправляет их всем подключённым WebSocket-клиентам.
/// Каждый тик гарантированно уникален благодаря атомарно инкрементируемому Trade ID.
/// </summary>
public class TickGeneratorService : BackgroundService
{
    private readonly Settings _settings;
    private readonly ILogger<TickGeneratorService> _logger;
    private readonly ConcurrentDictionary<string, ClientState> _clients = new();

    /// <summary>
    /// Сигнал, который срабатывает при первом подключении клиента.
    /// Используется, чтобы генерация не запускалась до появления хоть одного подписчика.
    /// </summary>
    private readonly TaskCompletionSource _firstClientConnected = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Потокобезопасный генератор случайных чисел (ThreadLocal).
    /// </summary>
    private static readonly ThreadLocal<Random> ThreadLocalRandom = new(
        () => new Random(Environment.TickCount));

    /// <summary>
    /// Множество всех сгенерированных Trade ID для runtime-валидации уникальности.
    /// </summary>
    private readonly ConcurrentDictionary<long, byte> _generatedTradeIds = new();

    /// <summary>Глобальный счётчик trade ID. Гарантирует уникальность каждого тика.</summary>
    private long _globalTradeId;

    /// <summary>Счётчик отправленных сообщений для RPS-мониторинга.</summary>
    private long _sentCount;

    /// <summary>Счётчик всех сгенерированных тиков за всё время работы сервера.</summary>
    private long _totalTicks;

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
        // Значение инициализируется один раз, далее только атомарно инкрементируется
        var random = ThreadLocalRandom.Value!;
        _globalTradeId = random.NextInt64(100_000_000, 1_000_000_000);
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

        // Сигнализируем ожидающему ExecuteAsync, что первый клиент подключился
        _firstClientConnected.TrySetResult();
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
    /// Общее количество сгенерированных тиков с момента запуска сервера.
    /// </summary>
    public long TotalTicksGenerated => Interlocked.Read(ref _totalTicks);

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

        // Ждём первого подключения клиента, чтобы не генерировать тики впустую
        _logger.LogInformation("Ожидание первого подключения клиента...");
        try
        {
            await _firstClientConnected.Task.WaitAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Сервер остановлен до подключения первого клиента");
            return;
        }

        _logger.LogInformation("Первый клиент подключился, запуск генерации тиков");

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
                                Interlocked.Increment(ref _totalTicks);
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
    /// Локальный счётчик для синтеза микросекундного смещения (0..999),
    /// чтобы каждый сгенерированный тик имел уникальный timestamp
    /// и не отбрасывался как дубликат по (ticker, exchange, timestamp).
    /// </summary>
    private long _timestampOffset;

    /// <summary>
    /// Генерирует JSON-тик в формате Binance trade stream.
    /// Каждый тик имеет уникальный trade ID (t) благодаря атомарному инкременту _globalTradeId.
    /// Совместимость с BinanceWebSocketClient.ProcessMessageAsync.
    /// </summary>
    private string GenerateTick(string symbol)
    {
        // Атомарно инкрементируем глобальный счётчик — это гарантирует,
        // что каждый тик получает уникальный Trade ID.
        var tradeId = Interlocked.Increment(ref _globalTradeId);

        // Runtime-валидация: проверяем, что такой Trade ID ещё не встречался.
        // В нормальной работе это условие никогда не должно срабатывать,
        // но на случай бага с multi-instance или переполнением счётчика — защита.
        if (!_generatedTradeIds.TryAdd(tradeId, 0))
        {
            _logger.LogWarning(
                "НАРУШЕНИЕ УНИКАЛЬНОСТИ: Trade ID {TradeId} уже был сгенерирован ранее! " +
                "Это может указывать на проблему в логике генерации.", tradeId);
        }

        // Синтезируем уникальный миллисекундный timestamp для каждого тика.
        // База — реальное unix-время в миллисекундах + атомарное смещение (0..999)
        // для гарантии уникальности каждого тика.
        //
        // Это критически важно: без смещения тики, сгенерированные в одной миллисекунде,
        // получают одинаковый timestamp и отбрасываются как дубликаты в MarketDataProcessor
        // (GroupBy по (Ticker, Exchange, Timestamp) + ON CONFLICT DO NOTHING).
        //
        // В реальной бирже Binance каждая сделка имеет уникальный T (ms), поэтому
        // использование синтетического timestamp имитирует реальное поведение.
        var offset = Interlocked.Increment(ref _timestampOffset) % 1000;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var syntheticTimestamp = nowMs + offset;

        var random = ThreadLocalRandom.Value!;

        // Случайное отклонение цены +/- 0.2%
        var priceVariation = 1.0 + (random.NextDouble() - 0.5) * 0.004;
        var price = _settings.BasePrice * (decimal)priceVariation;

        // Случайный объём: 0.0001 – 0.1 (для BTC) или 0.1 – 10 (для альткоинов)
        var isBtc = symbol.StartsWith("btc", StringComparison.OrdinalIgnoreCase);
        var volume = isBtc
            ? 0.0001m + (decimal)random.NextDouble() * 0.0999m
            : 0.1m + (decimal)random.NextDouble() * 9.9m;

        var isBuyerMaker = random.Next(2) == 0;
        var isBestMatch = random.Next(2) == 0;

        // Сериализуем в JSON (используем System.Text.Json)
        return JsonSerializer.Serialize(new BinanceTick
        {
            e = "trade",
            E = syntheticTimestamp,
            s = symbol.ToUpperInvariant(),
            t = tradeId,
            p = price,
            q = volume,
            T = syntheticTimestamp,
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
                var totalTicks = Interlocked.Read(ref _totalTicks);
                _logger.LogInformation(
                    "Статус: клиентов={Clients}, targetRps={TargetRps}, actualRps={ActualRps:F0}, всего тиков={TotalTicks}",
                    clients, targetRps, actualRps, totalTicks);
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
