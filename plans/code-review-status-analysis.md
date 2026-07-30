# Анализ статуса замечаний CODE_REVIEW_REPORT

Дата анализа: 2026-07-30  
Исходный отчёт: [`plans/CODE_REVIEW_REPORT (30).md`](plans/CODE_REVIEW_REPORT%20(30).md)

---

## Топ-10 проблем

### ✅ 1. Утечка EF Core `DbContext` / `ChangeTracker` — ИСПРАВЛЕНО

**Было:** Worker создавал один `IServiceScope` на весь жизненный цикл, `Scoped` `IMarketDataProcessor` держал `DbContext` часами/днями, `ChangeTracker` накапливал миллионы entities.

**Стало:** [`MarketDataProcessor`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) теперь **Singleton** (строка 121 в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:121)). В [`ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:566) создаётся **отдельный scope** на каждый батч через `_scopeFactory.CreateScope()`. `BulkCopyAsync` использует raw SQL (Binary COPY protocol), полностью обходя `ChangeTracker`. Нет `SaveChangesAsync` — данные пишутся через `ExecuteSqlRawAsync`/`ExecuteScalarAsync`.

**Оценка:** Проблема полностью решена. `DbContext` живёт только в рамках одного батча и диспозится после `using var scope`.

---

### ✅ 2. N+1 при дедупликации (`ExistsAsync` в цикле) — ИСПРАВЛЕНО

**Было:** 100 отдельных `ExistsAsync` запросов к PostgreSQL на каждый батч (100 round-trip'ов × 2-10ms = 200ms-1s).

**Стало:** Дедупликация теперь работает через in-memory [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) — FIFO-кэш на `Dictionary` с O(1) проверкой. В [`ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:536-559) используется единый проход: `dedupCache.Contains()` → `dedupCache.Add()`. Никаких запросов к БД для дедупликации. Additionally, [`BulkCopyAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:233) использует `ON CONFLICT DO NOTHING` как финальный рубеж защиты.

**Остаточная проблема:** `ExistsAsync` (строка 92 в [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:92)) всё ещё существует в интерфейсе, но **не вызывается в горячем пути**. [`DataStorageService`](src/MarketDataCollector.Application/Services/DataStorageService.cs:94) использует его — это legacy-код, не задействован в основном пайплайне.

**Оценка:** Горячий путь полностью переработан. Legacy-метод `ExistsAsync` в `DataStorageService` можно удалить, но на производительность не влияет.

---

### ❌ 3. Receive-loop fire-and-forget с гонкой при рестарте — НЕ ИСПРАВЛЕНО

**Было и осталось:** В [`BaseWebSocketClient.StartReceiveLoopAsync()`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:278):

```csharp
private async Task StartReceiveLoopAsync(CancellationToken cancellationToken)
{
    StopReceiveLoop();  // не await — старый loop может ещё крутиться
    _receiveLoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    _receiveLoopTask = _messageReceiver.StartReceiveLoopAsync(...);
    await Task.CompletedTask; // Fire-and-forget по-прежнему
}
```

[`StopReceiveLoop()`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:293) по-прежнему **пустой** — только `LogDebug` (строка 143 в [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:143)). Старый receive loop **не ожидается**, два цикла могут существовать одновременно. `_receiveLoopTask` не отслеживается.

**Критичность:** Высокая. При рестарте клиента возможны две параллельные итерации receive loop, конкурирующие за `IClientWebSocket.ReceiveAsync` и `MemoryStream`.

---

### ⚠️ 4. Обратное давление глушит WebSocket — ЧАСТИЧНО ИСПРАВЛЕНО

**Было:** `Channel.Writer.WriteAsync` блокировал receive loop при заполненном канале.

**Стало:** [`ProcessTickAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:143) теперь использует `TryWrite` с `BoundedChannelFullMode.DropOldest`:

```csharp
if (!channels[channelIndex].Writer.TryWrite(tick))
{
    Interlocked.Increment(ref _totalDroppedCount);
}
```

`TryWrite` **неблокирующий** — receive loop больше не блокируется. Дропы считаются через `_totalDroppedCount`.

**Оставшаяся проблема:** В [`WebSocketMessageReceiver`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:94) обработка сообщения по-прежнему идёт синхронно в receive loop:

```csharp
await processMessage(message);  // BinanceWebSocketClient.ProcessMessageAsync → _dataProcessor.ProcessTickAsync
```

Хотя `ProcessTickAsync` теперь неблокирующий (возвращает `Task.CompletedTask`), сам `await` создаёт state machine overhead на каждое сообщение. Это не критично при текущей архитектуре, но не является оптимальным.

**Оценка:** Основная проблема (блокировка receive loop) решена через `TryWrite` + `DropOldest`. Остался небольшой overhead от `await`.

---

### ⚠️ 5. `_processingTask` не наблюдается, ошибки батча не приводят к ретраю — ЧАСТИЧНО ИСПРАВЛЕНО

**Было:** `_ = marketDataProcessor.StartProcessingAsync(stoppingToken)` — fire-and-forget, батчи терялись молча.

**Стало:** В [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:56) задача сохраняется в `processorTask` и **наблюдается**:

```csharp
var processorTask = marketDataProcessor.StartProcessingAsync(stoppingToken);
// ...
if (processorTask.IsFaulted)
{
    throw new InvalidOperationException(
        "MarketDataProcessor background task failed",
        processorTask.Exception?.InnerException);
}
```

В [`ProcessBatchesAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:487-496) фатальные ошибки **пробрасываются** через `throw`, что fault'ит `_processingTask`.

**Оставшаяся проблема:** В [`ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:615-622) ошибки одного батча **всё ещё не приводят к ретраю** — батч теряется:

```csharp
catch (Exception ex)
{
    _logger.LogError(ex, "Критическая ошибка при обработке батча из {Count} тиков", batch.Count);
    // Временная ошибка — consumer продолжает работать. Исключение НЕ пробрасывается.
}
```

Это осознанное решение (fire-and-forget для транзиентных ошибок БД), но при долговременном отказе БД тики будут теряться.

**Оценка:** Наблюдение за `_processingTask` исправлено. Тихая потеря батчей при ошибках БД сохраняется — это **design decision**, но для production нужна retry-логика или dead-letter queue.

---

### ❌ 6. `SaveConnectionLogAsync` — неограниченный fire-and-forget — НЕ ИСПРАВЛЕНО

В [`MonitoringService.UpdateConnectionStatus()`](src/MarketDataCollector.Application/Services/MonitoringService.cs:86):

```csharp
_ = SaveConnectionLogAsync(exchange, eventType, dbMessage);
```

Каждое событие создаёт отдельный `IServiceScope` (строка 142) и fire-and-forget `Task`. Нет ограничения количества одновременных задач. При «шторме реконнектов» могут плодиться сотни задач.

**Критичность:** Средняя. При шторме реконнектов создаётся паразитная нагрузка на пул соединений БД и GC.

---

### ⚠️ 7. Sync-over-async в `Dispose` + race condition — ЧАСТИЧНО ИСПРАВЛЕНО

**Было и осталось:** [`Dispose()`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:434):

```csharp
StopAsync(CancellationToken.None).Wait(_options.DisposeTimeout); // sync-over-async
```

**Исправлено:** Добавлен `IAsyncDisposable` + `DisposeAsync()` (строка 449). При возможности использования `await using` проблема обходится.

**Осталось:**
1. Синхронный `Dispose()` всё ещё содержит `Wait()` — deadlock-prone в средах с `SynchronizationContext`.
2. [`DisposeCore()`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:460) обращается к `_backgroundRecoveryCts` и `_connectionManager` без синхронизации с `_backgroundLock`, хотя `StopAsync` уже обнулил `_backgroundRecoveryCts` под локом.

**Оценка:** `DisposeAsync` добавлен — это хорошее улучшение. Sync-over-async в `Dispose()` остался для обратной совместимости.

---

### ❌ 8. Неатомарная проверка/обновление состояния — НЕ ИСПРАВЛЕНО

1. **WebSocketConnectionManager.ConnectAsync** — [`Interlocked.Exchange` + мёртвое условие](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:62-64) всё ещё на месте:

```csharp
var oldWs = Interlocked.Exchange(ref _webSocket, ws);
if (oldWs != _webSocket) { try { oldWs.Dispose(); } catch { /* ignore */ } }
```

После `Interlocked.Exchange` `_webSocket` всегда равен `ws`, поэтому условие **всегда истинно**.

2. **MonitoringService.LogStatus** — [`foreach + индексация`](src/MarketDataCollector.Application/Services/MonitoringService.cs:124-129) без атомарности.

3. **События `Connected/Disconnected/ErrorOccurred`** — определены с `= null!` и вызываются как `MessageReceived?.Invoke(...)` с null-check через `?.`. Это **безопасно** — NullReferenceException невозможен.

**Оценка:** Мёртвый код в `WebSocketConnectionManager.ConnectAsync` остался. Логирование в `MonitoringService` содержит двойной снимок. Null-safety для событий исправлена через `?.Invoke`.

---

### ⚠️ 9. LINQ и лишние материализации — ЧАСТИЧНО ИСПРАВЛЕНО

**Было:** Группировка через `GroupBy` + `Select` + `ToList` на каждый батч, двойное перечисление в `DataStorageService`.

**Стало:** В горячем пути ([`ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:536-575)):
- `GroupBy` заменён на `DeduplicationCache` (один проход).
- `BulkCopyAsync` принимает готовый `List<RawTick>`, без лишних аллокаций.
- Единственное `ToList()` — для создания entities перед bulk insert.

**Осталось:** В [`DataStorageService.StoreRawTicksBatchAsync()`](src/MarketDataCollector.Application/Services/DataStorageService.cs:46) повторное перечисление:

```csharp
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks.Count());
```

`rawTicks.Count()` перечисляет `IEnumerable` повторно. Этот метод **не используется** в горячем пути (процессор пишет напрямую через репозиторий), но является legacy-кодом.

**Оценка:** Горячий путь оптимизирован. Legacy-код в `DataStorageService` содержит незначительные проблемы.

---

### ❌ 10. `ExponentialReconnectStrategy.ShouldRetry` всегда `true` + нет jitter — НЕ ИСПРАВЛЕНО

В [`ExponentialReconnectStrategy`](src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs):

```csharp
public bool ShouldRetry(int attempt) => true;  // бесконечно
public void Reset() => _logger.LogDebug("Сброс состояния стратегии переподключения.");
public TimeSpan GetDelay(int attempt)
{
    var delaySeconds = Math.Min(
        _options.ReconnectDelay.TotalSeconds * Math.Pow(2, attempt - 1),
        _options.MaxReconnectDelay.TotalSeconds);  // без jitter
    return TimeSpan.FromSeconds(delaySeconds);
}
```

Все три проблемы сохраняются:
1. `ShouldRetry` возвращает `true` всегда — `MaxInternalReconnectAttempts` не используется.
2. `Reset()` — no-op.
3. Нет **jitter** — при N клиентах все уйдут в одинаковые задержки → thundering herd.

**Критичность:** Высокая. При системном сбое Binance все клиенты переподключатся синхронно.

---

## Прочие замечания

| # | Замечание | Статус |
|---|-----------|--------|
| А | Отсутствие `.ConfigureAwait(false)` в Core/Application/Infrastructure | ❌ **Не исправлено** — ни одного `ConfigureAwait(false)` в проекте |
| Б | `JObject.Parse` вместо `System.Text.Json` в `BinanceWebSocketClient.ProcessMessageAsync` | ❌ **Не исправлено** — всё ещё `JObject.Parse` (строка 74 в [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:74)) |
| В | `Encoding.UTF8.GetString` аллоцирует строку на каждое сообщение | ❌ **Не исправлено** — строка 91 в [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:91) |
| Г | Дедупликация по `(Ticker, Exchange, Timestamp)` с миллисекундной меткой — легально разные трейды «слеплены» | ❌ **Не исправлено** — логика дедупликации та же самая |
| Д | Тесты дедупликации маскируют N+1 | ✅ **Исправлено** — горячий путь больше не использует `ExistsAsync`, тесты на `DeduplicationCache` корректны |
| Е | `public Channel<TickData> Channel => _channel` — нарушение инкапсуляции | ⚠️ **Частично исправлено** — заменён на `public Channel<TickData> GetChannel(int index = 0)` (строка 676). Свойство заменено методом, но доступ публичный |
| Ж | `RawTick.UpdatePrice/UpdateVolume` — мутабельные методы | ❌ **Не исправлено** — методы присутствуют (строки 65-73 в [`RawTick.cs`](src/MarketDataCollector.Domain/Entities/RawTick.cs:65)) |
| З | `Worker.RunHealthCheckAsync` дублирует ответственность с recovery-loop | ❌ **Не исправлено** — health-check по-прежнему вызывает `client.StartAsync()` для дисконнектнутых клиентов (строка 251 в [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:251)), параллельно с recovery-loop |
| И | `WebSocketClientFactory` — подписки на события не отписываются при остановке | ❌ **Не исправлено** — [`CreateBinanceClient()`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:79-94) добавляет обработчики событий, но не хранит ссылки для отписки |

---

## Сводная таблица

| # | Проблема | Статус | Критичность |
|---|----------|--------|-------------|
| 1 | Утечка DbContext / ChangeTracker | ✅ Исправлено | Высокая |
| 2 | N+1 при дедупликации | ✅ Исправлено | Высокая |
| 3 | Receive-loop fire-and-forget + гонка | ❌ Не исправлено | Высокая |
| 4 | Обратное давление блокирует WebSocket | ⚠️ Частично | Высокая |
| 5 | _processingTask не наблюдается | ⚠️ Частично | Высокая |
| 6 | SaveConnectionLogAsync без bounds | ❌ Не исправлено | Средняя |
| 7 | Sync-over-async в Dispose | ⚠️ Частично | Средняя |
| 8 | Неатомарная проверка состояния | ❌ Не исправлено | Средняя |
| 9 | LINQ и лишние аллокации | ⚠️ Частично | Низкая |
| 10 | ShouldRetry всегда true + нет jitter | ❌ Не исправлено | Высокая |
| А | ConfigureAwait(false) | ❌ Не исправлено | Низкая |
| Б | JObject.Parse | ❌ Не исправлено | Низкая |
| В | Encoding.UTF8.GetString аллокация | ❌ Не исправлено | Низкая |
| Г | Дедупликация по Timestamp | ❌ Не исправлено | Низкая |
| Д | Тесты дедупликации | ✅ Исправлено | — |
| Е | Инкапсуляция Channel | ⚠️ Частично | Низкая |
| Ж | RawTick мутабельные методы | ❌ Не исправлено | Низкая |
| З | Дублирование ответственности health-check | ❌ Не исправлено | Низкая |
| И | Утечка подписок на события | ❌ Не исправлено | Низкая |

**Итого:** 3 из 10 топ-проблем полностью исправлены, 4 частично исправлены, 3 не исправлены. Из 10 прочих замечаний — 1 исправлено, 1 частично, 8 не исправлены.
