using System.Collections.Concurrent;
using System.Globalization;
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

    /// <summary>Флаг: был ли уже залогирован первый тик.</summary>
    private bool _firstTickLogged;

    /// <summary>Флаг: достигнут ли лимит MaxTicks.</summary>
    private volatile bool _isLimitReached;

    /// <summary>Достигнут ли лимит MaxTicks.</summary>
    public bool IsLimitReached => _isLimitReached;

    /// <summary>Таймер для логирования статистики RPS.</summary>
    private DateTime _lastStatsTime = DateTime.UtcNow;
    private long _lastSentCount;

    /// <summary>
    /// Буфер последних сгенерированных JSON-тиков для каждого symbol.
    /// Используется для генерации дублей: с вероятностью DupPercent
    /// отправляется полная копия одного из последних 20 тиков.
    /// </summary>
    private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _recentJsons = new();

    /// <summary>
    /// Максимальный размер буфера _recentJsons для каждого symbol.
    /// Вычисляется как Rps / Symbols.Length * 2, минимум 1000.
    /// </summary>
    private readonly int _bufferSize;

    /// <summary>Счётчик отправленных дубликатов (полных копий JSON).</summary>
    private long _duplicateTicksSent;

    /// <summary>Сколько дубликатов (полных копий JSON) было отправлено.</summary>
    public long DuplicateTicksSent => Interlocked.Read(ref _duplicateTicksSent);

    /// <summary>
    /// Реальный процент дублей от общего числа отправленных тиков.
    /// </summary>
    public double ActualDupPercent => TotalTicksGenerated > 0
        ? (double)DuplicateTicksSent / TotalTicksGenerated * 100.0
        : 0.0;

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

        // Вычисляем размер буфера для дублей: ~2 секунды данных на symbol, минимум 1000
        _bufferSize = Math.Max(_settings.Rps / Math.Max(_settings.Symbols.Length, 1) * 2, 1000);

        if (_settings.DupPercent > 0)
        {
            _logger.LogInformation(
                "Режим дублей включён: DupPercent={DupPercent}%, bufferSize={BufferSize} на symbol",
                _settings.DupPercent, _bufferSize);
        }
    }

    /// <summary>
    /// Добавляет WebSocket-клиента в список получателей тиков.
    /// Возвращает сгенерированный ID клиента для последующего удаления.
    /// </summary>
    public string AddClient(WebSocket webSocket, string symbol)
    {
        var clientId = Guid.NewGuid().ToString("N")[..8];
        _clients.TryAdd(clientId, new ClientState(webSocket, symbol));
        _logger.LogInformation("Клиент {ClientId} подключился. Всего клиентов: {Count}, symbol: {Symbol}",
            clientId, _clients.Count, symbol);

        // Сигнализируем ожидающему ExecuteAsync, что первый клиент подключился
        _firstClientConnected.TrySetResult();
        return clientId;
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
                // Проверка лимита MaxTicks — если достигнут, спим и не генерируем
                if (_settings.MaxTicks > 0 && Interlocked.Read(ref _totalTicks) >= _settings.MaxTicks)
                {
                    if (!_isLimitReached)
                    {
                        _isLimitReached = true;
                        _logger.LogInformation(
                            "Достигнут лимит тиков: {MaxTicks}. Генерация остановлена, сервис продолжает работу.",
                            _settings.MaxTicks);
                    }
                    await Task.Delay(1000, stoppingToken);
                    continue;
                }

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

                                // Логируем первый сгенерированный тик
                                if (!_firstTickLogged)
                                {
                                    _firstTickLogged = true;
                                    _logger.LogInformation(
                                        "ПЕРВЫЙ ТИК: symbol={Symbol}, tradeId={TradeId}, price={Price}, volume={Volume}, json={TickJson}",
                                        clientState.Symbol,
                                        Interlocked.Read(ref _globalTradeId),
                                        _settings.BasePrice,
                                        tickJson.Length,
                                        tickJson);
                                }

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

        var finalTotalTicks = Interlocked.Read(ref _totalTicks);
        _logger.LogInformation("TickGenerator остановлен. Всего сгенерировано тиков: {TotalTicks}", finalTotalTicks);
    }

    /// <summary>
    /// Генерирует JSON-тик в формате Binance trade stream.
    /// Если включён режим дублей (DupPercent > 0), с заданной вероятностью
    /// возвращает полную копию одного из последних 20 тиков для этого symbol.
    /// Иначе — генерирует новый уникальный тик и сохраняет его в буфер.
    /// </summary>
    private string GenerateTick(string symbol)
    {
        var random = ThreadLocalRandom.Value!;

        // Режим дубля: с вероятностью DupPercent берём случайный JSON из последних 20
        if (_settings.DupPercent > 0 && random.Next(100) < _settings.DupPercent)
        {
            if (_recentJsons.TryGetValue(symbol, out var queue) && !queue.IsEmpty)
            {
                var snapshot = queue.ToArray();
                var maxIndex = Math.Min(20, snapshot.Length);
                if (maxIndex > 0)
                {
                    var dupJson = snapshot[random.Next(maxIndex)];
                    Interlocked.Increment(ref _duplicateTicksSent);
                    return dupJson; // полная копия JSON — тот же tradeId, timestamp, цена, объём
                }
            }
        }

        // Нормальный режим: генерируем новый уникальный тик
        var tradeId = Interlocked.Increment(ref _globalTradeId);

        // Runtime-валидация: проверяем, что такой Trade ID ещё не встречался.
        if (!_generatedTradeIds.TryAdd(tradeId, 0))
        {
            _logger.LogWarning(
                "НАРУШЕНИЕ УНИКАЛЬНОСТИ: Trade ID {TradeId} уже был сгенерирован ранее! " +
                "Это может указывать на проблему в логике генерации.", tradeId);
        }

        // Timestamp = tradeId (строго монотонный)
        var syntheticTimestamp = tradeId;

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
        var json = JsonSerializer.Serialize(new BinanceTick
        {
            e = "trade",
            E = syntheticTimestamp,
            s = symbol.ToUpperInvariant(),
            t = tradeId,
            p = price.ToString("F8", CultureInfo.InvariantCulture),
            q = volume.ToString("F8", CultureInfo.InvariantCulture),
            T = syntheticTimestamp,
            m = isBuyerMaker,
            M = isBestMatch
        });

        // Сохраняем JSON в буфер для последующего использования как дубль
        var symbolQueue = _recentJsons.GetOrAdd(symbol, _ => new ConcurrentQueue<string>());
        symbolQueue.Enqueue(json);

        // Поддерживаем максимальный размер очереди (удаляем старые записи)
        while (symbolQueue.Count > _bufferSize && symbolQueue.TryDequeue(out _)) { }

        return json;
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
                var totalDuplicates = Interlocked.Read(ref _duplicateTicksSent);
                var uniqueTicks = totalTicks - totalDuplicates;
                _logger.LogInformation(
                    "Статус: клиентов={Clients}, targetRps={TargetRps}, actualRps={ActualRps:F0}, " +
                    "всего={TotalTicks}, уникальных={UniqueTicks}, дублей={Duplicates} ({DupPercent:F1}%)",
                    clients, targetRps, actualRps, totalTicks, uniqueTicks, totalDuplicates,
                    totalTicks > 0 ? (double)totalDuplicates / totalTicks * 100.0 : 0.0);
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
        public string p { get; init; } = "0";
        public string q { get; init; } = "0";
        public long T { get; init; }
        public bool m { get; init; }
        public bool M { get; init; }
    }
}
