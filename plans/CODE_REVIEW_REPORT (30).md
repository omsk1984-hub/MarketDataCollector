# Code Review Report — MarketDataCollector

Фокус ревью: многопоточность, асинхронность и поведение под высокой нагрузкой (50–100 тиков/сек, 2–3+ источника). Решения по проблемам в отчёте намеренно не предлагаются.

---

## Общая оценка уровня кандидата

Уровень — **уверенный Middle**.

Положительные стороны:
- Чистая многоуровневая архитектура (Core / Application / Domain / Infrastructure / Workers), DI, конфигурация через `IOptions<T>`.
- Корректные базовые примитивы для высокой нагрузки: `Channel<T>` с ограниченной ёмкостью и `BoundedChannelFullMode.Wait`, `ArrayPool<byte>` в receive‑loop, `SemaphoreSlim` для сериализации подключений, `Interlocked.Exchange` при подмене сокета, `ConcurrentDictionary` для статусов, уникальный индекс в БД как последний рубеж дедупликации.
- Двухфазная инициализация для разрыва циклической зависимости `Client ↔ SubscriptionManager`, фабрика клиентов, расширяемость под новые биржи.
- Идемпотентный `ConnectAsync`/`StartAsync`, отдельный health‑check цикл в Worker, использование Polly для подписки.
- Есть юнит‑тесты на ключевые сценарии.

Слабые стороны (именно под нагрузку):
- Архитектурно правильные «коробочки», но внутренние реализации горячего пути (дедупликация в БД, время жизни `DbContext`, обработка ошибок batch‑процессинга) рассчитаны на демо, а не на 50–100 tps в течение часов.
- Есть несколько системных конкурентных проблем (необсервабельные фоновые задачи, гонки при рестарте receive‑loop, sync‑over‑async в `Dispose`).
- Нет реальной нагрузочной проверки/бенчмарка; настройки тестов и моки маскируют узкие места.

В целом — это кандидат, который умеет писать «правильный» каркас, но ещё не умеет доводить его до production‑grade под реальной нагрузкой.

---

## Топ‑10 проблем (по убыванию критичности и масштаба)

### 1. Утечка EF Core `DbContext` / `ChangeTracker` на всё время жизни процесса

Файл: [src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs), [src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs), [src/MarketDataCollector.Application/Services/MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs)

Worker создаёт **один** `IServiceScope` на весь жизненный цикл `ExecuteAsync` и достаёт из него `Scoped` `IMarketDataProcessor`, который в свою очередь держит `Scoped` `IRawTickRepository` → `MarketDataDbContext`. Этот `DbContext` живёт часами/днями, через него идёт весь поток тиков, и его `ChangeTracker` неограниченно накапливает прикреплённые сущности `RawTick`. На потоке 50–100 tps это:
- линейный рост памяти процесса (сотни тысяч → миллионы trackable entities в день);
- деградация `SaveChangesAsync` (она становится O(N) по числу tracked entities, фактически квадратичной по совокупному времени);
- риск падения процесса по OOM раньше, чем сработают «крутые» механизмы переподключения.

```csharp
// Program.cs
builder.Services.AddDbContext<MarketDataDbContext>(...);
builder.Services.AddScoped<IRawTickRepository, RawTickRepository>();
builder.Services.AddScoped<IMarketDataProcessor>(sp => new MarketDataProcessor(...));

// Worker.cs
private async Task RunWithRecoveryAsync(CancellationToken stoppingToken)
{
    using var scope = _scopeFactory.CreateScope();          // один scope на всё время
    var marketDataProcessor = scope.ServiceProvider.GetRequiredService<IMarketDataProcessor>();
    ...
    _ = marketDataProcessor.StartProcessingAsync(stoppingToken); // и фоновая обработка идёт через него
}
```

Сюда же — отсутствие `AsNoTracking`/`ChangeTracker.Clear()`/пересоздания контекста между батчами.

---

### 2. N+1 при дедупликации (`ExistsAsync` в цикле по каждому тику)

Файл: [src/MarketDataCollector.Application/Services/MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs), [src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs)

Для каждого батча из 100 тиков делается до 100 синхронных по логике (последовательных по `await`) запросов в PostgreSQL. При сетевом RTT 2–10 мс это 200 мс–1 с **только на дедуп** одного батча, при 50–100 tps пайплайн физически не успевает обрабатывать поток → канал заполняется до `ChannelCapacity` → срабатывает `BoundedChannelFullMode.Wait` → блокируется WebSocket‑receive (см. п.4).

```csharp
private async Task<HashSet<(string, string, DateTime)>> GetExistingKeysFromDbAsync(
    List<TickData> ticks, CancellationToken cancellationToken)
{
    var existing = new HashSet<(string, string, DateTime)>();
    foreach (var tick in ticks)               // 100 итераций
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (await _rawTickRepository.ExistsAsync(            // 100 round-trip'ов
                tick.Ticker, tick.Exchange, tick.Timestamp, cancellationToken))
        {
            existing.Add((tick.Ticker, tick.Exchange, tick.Timestamp));
        }
    }
    return existing;
}
```

---

### 3. Receive‑loop запущен в режиме fire‑and‑forget с гонкой при рестарте

Файл: [src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs), [src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs)

`StartReceiveLoopAsync` сохраняет `_receiveLoopTask`, но **никогда его не ожидает** и не наблюдает исключения. При обнаружении дисконнекта recovery‑loop вызывает `ConnectAsync` повторно, тот снова дергает `StartReceiveLoopAsync`, который вызывает `StopReceiveLoop()` — а это синхронный метод, он лишь отменяет `CancellationTokenSource` и обнуляет поля, **не дожидаясь завершения предыдущей итерации цикла**. Итог: два цикла приёма могут существовать одновременно, конкурировать за `IClientWebSocket.ReceiveAsync` и за общий `MemoryStream`. Плюс `_receiveLoopCts`/`_receiveLoopTask` читаются/пишутся вне `_backgroundLock` из разных потоков.

```csharp
private async Task StartReceiveLoopAsync(CancellationToken cancellationToken)
{
    StopReceiveLoop();                          // не await — старый loop может ещё крутиться
    _receiveLoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    _receiveLoopTask = _messageReceiver.StartReceiveLoopAsync(...); // task никем не наблюдается
    await Task.CompletedTask;                   // Fire-and-forget by design
}

private void StopReceiveLoop()
{
    _messageReceiver.StopReceiveLoop();         // внутри только лог
    _receiveLoopCts?.Cancel();
    _receiveLoopCts?.Dispose();                 // dispose CTS, на которой ещё ждёт ReceiveAsync
    _receiveLoopCts = null;
    _receiveLoopTask = null;                    // ссылка теряется, исключения никто не увидит
}
```

`WebSocketMessageReceiver.StopReceiveLoop()` фактически пустой — только `LogDebug`, никакой реальной остановки нет.

---

### 4. Цепочка обратного давления глушит WebSocket и приводит к каскадным переподключениям

Файл: [src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs), [src/MarketDataCollector.Application/Services/MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs), [src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs)

Обработка одного сообщения вызывается прямо из receive‑loop через `await processMessage(message)`. В `BinanceWebSocketClient.ProcessMessageAsync` это `_dataProcessor.ProcessTickAsync` → `_channel.Writer.WriteAsync`, который при заполненном канале **блокируется** (`FullMode = Wait`). Это значит: как только БД/дедуп тормозит (см. п.1 и п.2) → канал заполнен → `WriteAsync` ждёт → цикл `ReceiveAsync` не делает следующий `Receive` → сервер Binance не получает понг, рвёт соединение → срабатывает recovery → новые подключения, ещё больше очереди ConnectionLog в БД → деградация ускоряется. Никакого «drop oldest»/метрики переполнения/ограничения времени `WriteAsync` нет.

```csharp
// WebSocketMessageReceiver
onMessageReceived?.Invoke(message);
await processMessage(message);     // блокирует приём следующего фрейма
```
```csharp
// MarketDataProcessor
public async Task ProcessTickAsync(...)
{
    await _channel.Writer.WriteAsync(new TickData(...));   // блокирует receive-loop
}
```

---

### 5. `_processingTask` не наблюдается, ошибки батча не приводят к ретраю — тихая потеря данных

Файл: [src/MarketDataCollector.Application/Services/MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs), [src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs)

`Worker` запускает обработку как `_ = marketDataProcessor.StartProcessingAsync(stoppingToken);` — без ожидания. Внутри `ProcessBatchesAsync` любой непредвиденный сбой в `ProcessBatchAsync` (DbUpdateException, таймаут, потеря соединения с БД) ловится общим `catch (Exception)`, вызывается `OnError`, но **батч просто теряется**, цикл продолжается, тики из этого батча не повторяются и не возвращаются в очередь. При высоком темпе тиков и любых проблемах с БД получаем массированную потерю данных без явного сигнала о её объёме.

```csharp
catch (Exception ex)
{
    _logger.LogError(ex, "Batch processing error: {Count} ticks", batch.Count);
    OnError?.Invoke(this, ex);            // не rethrow, цикл едет дальше, батч исчез
}
```

Дополнительно: `_processingTask = ProcessBatchesAsync(cancellationToken);` создаётся без `Task.Run`/без обёртки логирования и не передаётся в централизованный supervisor — `Worker` узнаёт о фатальной ошибке только через event `OnError` (одноразовый), а сам `Task` остаётся ненаблюдаемым.

---

### 6. `SaveConnectionLogAsync` — неограниченный fire‑and‑forget с созданием DI‑scope на каждое событие

Файл: [src/MarketDataCollector.Application/Services/MonitoringService.cs](src/MarketDataCollector.Application/Services/MonitoringService.cs)

`UpdateConnectionStatus` вызывается на каждое подключение/отключение/ошибку, причём из событий WebSocket клиента (которые во время «шторма реконнектов» могут сыпаться сотнями в секунду). Каждое событие порождает:
- отдельный `IServiceScope` (с собственным `DbContext`),
- асинхронную запись `INSERT` в `connectionlogs`,
- никем не наблюдаемый `Task`.

Количество одновременно выполняющихся «висящих» задач ничем не ограничено, ошибки только логируются, при отказе БД они продолжают плодиться → давление на пул соединений и GC, паразитная нагрузка как раз тогда, когда система и так в плохом состоянии.

```csharp
public void UpdateConnectionStatus(string exchange, ConnectionStatus status, string? message = null)
{
    _connectionStatuses[exchange] = status;
    ...
    _ = SaveConnectionLogAsync(exchange, status.ToString(), message);   // fire-and-forget, без bound'a
}

private async Task SaveConnectionLogAsync(...)
{
    using var scope = _scopeFactory.CreateScope();     // новый DbContext на каждое событие
    var repo = scope.ServiceProvider.GetService(typeof(IConnectionLogRepository)) as IConnectionLogRepository;
    ...
    await repo.AddAsync(log);
    await repo.SaveChangesAsync();
}
```

---

### 7. Sync‑over‑async в `Dispose` + неконсистентный жизненный цикл

Файл: [src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs)

`Dispose()` блокирует поток через `StopAsync(...).Wait(_options.DisposeTimeout)`. Это классический dead‑lock паттерн при наличии любого `SynchronizationContext`/в некоторых тест‑раннерах и при вызове из финализаторов/`using` в синхронных путях. Кроме того, `DisposeCore` отменяет `_backgroundRecoveryCts` после того, как `StopAsync` уже его обнулил под `_backgroundLock`, и обращается к закрытому/диспоунутому ресурсу `_connectionManager` без синхронизации с продолжающим работать receive‑loop.

```csharp
public virtual void Dispose()
{
    ...
    StopAsync(CancellationToken.None).Wait(_options.DisposeTimeout); // sync-over-async, deadlock-prone
    ...
    DisposeCore();
}

private void DisposeCore()
{
    StopReceiveLoop();
    _backgroundRecoveryCts?.Cancel();    // race с StopAsync, который уже обнулил поля
    _backgroundRecoveryCts?.Dispose();
    _connectionManager.StateChanged -= OnConnectionStateChanged;
    (_connectionManager as IDisposable)?.Dispose();
}
```

---

### 8. Неатомарная проверка/обновление состояния и события вне локов

Файлы: [src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs), [src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs), [src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs)

- `ConnectAsync` проверяет `IsConnected` без блокировки, между проверкой и фактическим вызовом `_connectionManager.ConnectAsync` состояние может измениться → одновременные подключения попадают в `SemaphoreSlim` ниже по стеку, но `OnConnected()`/`StartReceiveLoopAsync` могут вызываться в неконсистентной последовательности при параллельном `Connect`/`Stop`.
- В `WebSocketConnectionManager.ConnectAsync` логика `if (oldWs != _webSocket) Dispose()` бессмысленна: после `Interlocked.Exchange(ref _webSocket, ws)` поле `_webSocket` всегда указывает на `ws`, а `oldWs` — на предыдущее значение, поэтому условие почти всегда истинно (а если бы было ложно — был бы реальный double‑dispose). Это говорит о неаккуратной работе с конкурентным состоянием.
- События `Connected/Disconnected/MessageReceived/ErrorOccurred` инициализированы `= null!` и вызываются как `MessageReceived.Invoke(...)` без null‑check внутри `OnMessageReceived` (исходное определение `event ... = null!`). При гонке отписки от событий (в health‑check рестарте/Dispose) возможен `NullReferenceException`.
- `MonitoringService.LogStatus` итерирует `_connectionStatuses.Keys` и затем индексирует словарь — это два независимых снимка, плюс `Keys` аллоцирует промежуточную коллекцию каждые 30 секунд.

```csharp
// WebSocketConnectionManager.ConnectAsync — выглядит как мёртвый код / неверное условие
var oldWs = Interlocked.Exchange(ref _webSocket, ws);
if (oldWs != _webSocket) { try { oldWs.Dispose(); } catch { /* ignore */ } }
```

```csharp
// MonitoringService.LogStatus
foreach (var exchange in _connectionStatuses.Keys)  // снапшот Keys
{
    var status = _connectionStatuses[exchange];     // второй заход в словарь
    ...
}
```

---

### 9. LINQ и лишние материализации в горячем пути обработки тиков

Файл: [src/MarketDataCollector.Application/Services/MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs), [src/MarketDataCollector.Application/Services/DataStorageService.cs](src/MarketDataCollector.Application/Services/DataStorageService.cs)

На каждый батч (десятки раз в секунду) выполняется цепочка LINQ с аллокацией промежуточных коллекций и словарей:

```csharp
var uniqueTicks = batch
    .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))  // внутренний Dictionary + Grouping
    .Select(g => g.First())
    .ToList();                                          // List #1

var existingKeys = await GetExistingKeysFromDbAsync(uniqueTicks, cancellationToken);

var newTicks = uniqueTicks
    .Where(t => !existingKeys.Contains((t.Ticker, t.Exchange, t.Timestamp)))
    .ToList();                                          // List #2

var entities = newTicks.Select(t => new RawTick(...)).ToList(); // List #3
```

Плюс боксинг `ValueTuple` как ключа словаря с реализацией `GetHashCode` по полям, плюс `Interlocked.Add(... entities.Count)` и проверка `count % 100 < entities.Count` для логирования — это давление на GC именно тогда, когда система должна успевать 50–100 tps.

В `DataStorageService.StoreRawTicksBatchAsync` присутствует двойное перечисление `IEnumerable<RawTick>`:
```csharp
await _rawTickRepository.AddRangeAsync(rawTicks);
await _rawTickRepository.SaveChangesAsync();
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks.Count());  // повторное перечисление
```

---

### 10. `ExponentialReconnectStrategy.ShouldRetry` всегда `true` + одноминутный потолок без джиттера

Файл: [src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs](src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs), [src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs)

`ShouldRetry` возвращает `true` всегда; `Reset()` ничего не сбрасывает (только лог). Соответственно поле `MaxInternalReconnectAttempts` в конфиге фактически не используется. На рынке стандартная ситуация — массовый рестарт всех клиентов одновременно после системного сбоя у поставщика: при N=5 символах все они уйдут в одинаковые `5/10/20/40/60` секунд **без рандомного джиттера** → «thundering herd», синхронные всплески нагрузки на собственную БД (через ConnectionLog) и на чужой WS‑сервер, риск немедленного бана/throttle.

```csharp
public bool ShouldRetry(int attempt) => true;          // бесконечно
public void Reset() => _logger.LogDebug("Reconnect strategy reset.");   // no-op
public TimeSpan GetDelay(int attempt)
{
    var delaySeconds = Math.Min(
        _options.ReconnectDelay.TotalSeconds * Math.Pow(2, attempt - 1),
        _options.MaxReconnectDelay.TotalSeconds);     // без jitter
    return TimeSpan.FromSeconds(delaySeconds);
}
```

Плюс в `BaseWebSocketClient.RunBackgroundRecoveryLoopAsync` нормальный путь «коннект жив» — это `await Task.Delay(1000, ...)` в опросном цикле: дисконнект замечается с задержкой до 1 секунды, а сам индикатор `IsConnected` основан на `WebSocketState.Open`, который для «полу‑мертвого» сокета (нет данных, но статус Open) долго остаётся истинным.

---

## Прочие замечания (вне топ‑10, фиксирую коротко)

- Везде отсутствует `.ConfigureAwait(false)` в библиотечном коде Core/Application/Infrastructure.
- `BinanceWebSocketClient.ProcessMessageAsync` парсит JSON через `JObject.Parse` (Newtonsoft) — на горячем пути это значительно дороже `System.Text.Json` + `Utf8JsonReader`; плюс лишние аллокации строк (`.ToString()`, `decimal.Parse`).
- `Encoding.UTF8.GetString(...)` в receive‑loop аллоцирует новую строку на каждое сообщение (неизбежная цена при таком JSON‑пайплайне, но усугубляет п.9).
- Дедупликация по ключу `(Ticker, Exchange, Timestamp)` с миллисекундной меткой Binance: два разных трейда могут прийти с одинаковой меткой и быть **легально разными** — текущая логика их «слепит» как дубликаты и потеряет.
- Тесты дедупликации используют `SetupSequence(... ExistsAsync ...).ReturnsAsync(false).ReturnsAsync(true)` — это маскирует N+1 (см. п.2), правильный батч‑метод тестами не вынуждается.
- `MarketDataProcessor` хранит публичное свойство `public Channel<TickData> Channel => _channel;` — нарушение инкапсуляции, любая внешняя сторона может писать/читать канал, ломая инварианты.
- `RawTick.UpdatePrice`/`UpdateVolume` — мутабельные методы для сущности, помеченной уникальным индексом по `(Ticker, Exchange, Timestamp)`; они не используются осмысленно, но открывают дверь к гонкам.
- `Worker.RunHealthCheckAsync` вызывает `client.StartAsync(stoppingToken)` для дисконнектнутых клиентов параллельно с тем, что recovery‑loop того же клиента уже работает и сам пытается переподключиться — `StartAsync` идемпотентен, но это явное дублирование ответственности (две системы, оба думают, что они источник истины о рестарте).
- В `WebSocketClientFactory.CreateAllClients` подписки на события (`Connected`, `Disconnected`, ...) добавляются, но **никогда не отписываются** при остановке клиента → утечка ссылок на `MonitoringService` если когда‑нибудь добавится пересоздание клиентов.

---

## Итог по производительности

При заявленных 50–100 tps и текущей реализации основной потолок задаст связка п.1 + п.2 (раздутый `ChangeTracker` и N+1 при дедупликации). До этого потолка код, скорее всего, доедет, но не выдержит длительного прогона: появятся накопление памяти, рост латентности `SaveChanges`, заполнение канала, обратное давление на WS (п.4), реконнект‑шторм без джиттера (п.10) и тихие потери целых батчей (п.5). Конкурентные дефекты (п.3, п.7, п.8) добавят редких, плохо воспроизводимых багов в продакшене.
