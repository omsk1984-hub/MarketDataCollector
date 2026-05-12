# План: Ограничение времени тестов и диагностика висящих тестов

## Проблема
Один из тестов в проекте `MarketDataCollector.Tests` зависает без какого-либо вывода. Невозможно определить, какой именно тест вызывает зависание.

## Решение (Вариант C — Комбинированный)

План включает **3 компонента**, работающие вместе:

---

### 1. xunit.runner.json — глобальная диагностика выполнения тестов

**Файл:** `tests/MarketDataCollector.Tests/xunit.runner.json`

Включит диагностический режим xUnit, который выводит в консоль имя **каждого теста в момент его старта**. Когда тест зависнет, последнее выведенное имя укажет на висящий тест.

```json
{
  "$schema": "https://xunit.net/schema/current/xunit.runner.schema.json",
  "diagnosticMessages": true,
  "longRunningTestSeconds": 30
}
```

- `diagnosticMessages: true` — выводит `Starting: TestName`, `Finished: TestName` в консоль
- `longRunningTestSeconds: 30` — предупреждает о тестах, выполняющихся дольше 30 секунд

---

### 2. ITestOutputHelper — логирование из тестов

Добавить `ITestOutputHelper` в конструкторы **всех тестовых классов** и выводить имя теста при старте через `_output.WriteLine($"=== Running: {nameof(TestMethod)} ===")`.

**Затрагиваемые файлы (9 классов):**

| # | Файл | Тестовый класс | Риск зависания |
|---|------|---------------|----------------|
| 1 | `Core/Clients/BaseWebSocketClientTests.cs` | `BaseWebSocketClientTests` | 🔴 Высокий |
| 2 | `Core/Clients/WebSocketMessageReceiverTests.cs` | `WebSocketMessageReceiverTests` | 🔴 Высокий |
| 3 | `Core/Clients/SubscriptionManagerTests.cs` | `SubscriptionManagerTests` | 🟡 Средний |
| 4 | `Core/Clients/WebSocketConnectionManagerTests.cs` | `WebSocketConnectionManagerTests` | 🟡 Средний |
| 5 | `Infrastructure/Clients/BinanceWebSocketClientTests.cs` | `BinanceWebSocketClientTests` | 🔴 Высокий |
| 6 | `Application/Services/MarketDataProcessorTests.cs` | `MarketDataProcessorTests` | 🟡 Средний |
| 7 | `Application/Services/MonitoringServiceTests.cs` | `MonitoringServiceTests` | 🟡 Средний |
| 8 | `Infrastructure/Factories/WebSocketClientFactoryTests.cs` | `WebSocketClientFactoryTests` | 🟡 Средний |
| 9 | `Infrastructure/Repositories/RawTickRepositoryTests.cs` | `RawTickRepositoryTests` | 🟢 Низкий |

**Пример изменений в классе:**
```csharp
public class BaseWebSocketClientTests
{
    private readonly ITestOutputHelper _output;
    
    public BaseWebSocketClientTests(ITestOutputHelper output)
    {
        _output = output;
        // ... existing code ...
    }
    
    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WhenNotConnected_CallsConnectionManager()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenNotConnected_CallsConnectionManager)} ===");
        // ... existing test code ...
    }
}
```

---

### 3. Timeout на async-тестах

xUnit поддерживает параметр `[Fact(Timeout = <ms>)]`. Если тест превышает таймаут, он прерывается и падает с `TimeoutException`, что позволяет сразу определить висящий тест по его имени в отчёте.

**Рекомендуемый таймаут:** `5000` мс (5 секунд) для async-тестов.
Для синхронных тестов таймаут не требуется — они мгновенные.

#### Полный список тестов для таймаута:

**BaseWebSocketClientTests (6 тестов):**
- `ConnectAsync_WhenAlreadyConnected_DoesNothing` — async
- `ConnectAsync_WhenNotConnected_CallsConnectionManagerAndStartsReceiveLoop` — async
- `ConnectAsync_WithSubscriptionManager_CallsSubscribeWithRetryAsync` — async
- `StopAsync_StopsBackgroundRecoveryLoop` — async (содержит `Task.Delay(100)`)
- `DisconnectAsync_WhenConnected_ClosesConnection` — async
- `DisconnectAsync_WhenNotConnected_DoesNothing` — async
- `DisposeAsync_DisposesResources` — async

**WebSocketMessageReceiverTests (6 тестов):**
- `StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage` — async
- `StartReceiveLoopAsync_ConnectionLost_BreaksLoop` — async
- `StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage` — async
- `StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError` — async
- `StartReceiveLoopAsync_ProcessMessageThrows_CallsOnError` — async
- `StartReceiveLoopAsync_ReceiveCloseMessage_BreaksLoop` — async
- `StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop` — async

**SubscriptionManagerTests (5 тестов):**
- `SubscribeWithRetryAsync_Success_NoRetries` — async
- `SubscribeWithRetryAsync_ThrowsException_RetriesUpToMax` — async
- `SubscribeWithRetryAsync_AllRetriesExhausted_Throws` — async
- `SubscribeWithRetryAsync_RetryDelayIsExponential` — async
- `SubscribeWithRetryAsync_SymbolPassedToAction` — async

**WebSocketConnectionManagerTests (7 тестов):**
- `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects` — async
- `ConnectAsync_WhenAlreadyConnected_DoesNothing` — async
- `DisconnectAsync_WhenOpen_ClosesGracefully` — async
- `DisconnectAsync_WhenClosed_DoesNothing` — async
- `DisconnectAsync_WhenCloseThrows_LogsWarning` — async
- `SendAsync_WhenNotOpen_ThrowsInvalidOperationException` — async
- `SendAsync_WhenOpen_SendsMessage` — async
- `ReceiveAsync_WhenNotOpen_ThrowsInvalidOperationException` — async
- `ReceiveAsync_WhenOpen_ReturnsResult` — async

**BinanceWebSocketClientTests (все async тесты, ~10+ тестов):**
- Все тесты, помеченные `async Task` — HIGH RISK

**MarketDataProcessorTests (все async тесты, ~5+ тестов):**
- Тесты с `ProcessTickAsync`, `FlushAsync` и т.д. — MEDIUM RISK

**WebSocketClientFactoryTests (все async тесты, ~3+ теста):**
- Тесты с `CreateClientsAsync` — MEDIUM RISK

**RawTickRepositoryTests (все async тесты, ~10+ тестов):**
- Тесты с EF Core InMemory — от 5 до 10 секунд

---

### 4. Обновление скрипта run.ps1

Добавить флаг `--verbosity detailed` (или `-v d`) для более подробного вывода:

```powershell
dotnet test MarketDataCollector.Tests.csproj --no-build --verbosity detailed
```

---

## Итоговая схема работы

```mermaid
flowchart TD
    A[dotnet test] --> B[xunit.runner.json]
    B --> C[diagnosticMessages: true]
    B --> D[longRunningTestSeconds: 30]
    C --> E{Вывод в консоль}
    D --> F{Предупреждение о долгих тестах}
    
    E --> G[Starting: TestName A]
    E --> H[Starting: TestName B]
    H --> I[Starting: TestName C]
    I --> J{Test C hangs?}
    J -->|Yes| K[Последний вывод: Starting: TestName C]
    J -->|No| L[Finished: TestName C]
    
    M[Timeout: 5000ms на async тестах] --> N[Тест упадёт с TimeoutException]
    N --> O[Имя теста в отчёте]
    
    P[ITestOutputHelper] --> Q[Доп. логирование внутри теста]
```

## Порядок реализации

1. Создать `tests/MarketDataCollector.Tests/xunit.runner.json`
2. Добавить `ITestOutputHelper` во все 9 тестовых классов (только в конструктор, с сохранением в поле)
3. Добавить `_output.WriteLine(...)` перед каждым **async** тестом
4. Добавить `[Fact(Timeout = 5000)]` на все async-тесты в **высокорисковых классах**
5. Добавить `[Fact(Timeout = 10000)]` на async-тесты в **среднерисковых классах**
6. Обновить `tests/run_test.ps1`

---

## 5. Правила и шаблоны для написания новых тестов

Каждый новый тестовый класс и тестовый метод должны следовать установленному стилю диагностики и таймаутов. Это обеспечивает единообразие и предотвращает появление неотловленных висящих тестов.

### 5.1. Обязательные компоненты для нового тестового класса

1. **Конструктор с `ITestOutputHelper`** — всегда добавлять параметр `ITestOutputHelper output` в конструктор
2. **Поле `_output`** — сохранять helper в приватное поле `private readonly ITestOutputHelper _output;`
3. **Диагностический вывод** — каждый **async** тест должен начинаться с `_output.WriteLine($"=== Running: {nameof(TestMethodName)} ===");`
4. **Таймаут на async-тестах** — каждый **async** тест должен иметь `[Fact(Timeout = <ms>)]`

### 5.2. Определение уровня таймаута

| Риск | Таймаут | Категории тестов |
|------|---------|-----------------|
| 🔴 Высокий | `Timeout = 5000` (5 сек) | WebSocket-клиенты, Receive loop, Subscription, Connection Manager, BinanceWebSocketClient |
| 🟡 Средний | `Timeout = 10000` (10 сек) | Repository (EF Core InMemory), DataStorage, MarketDataProcessor |
| 🟢 Низкий | без таймаута | Синхронные тесты (не `async Task`), MonitoringService, WebSocketClientFactory |

### 5.3. Шаблон нового тестового класса

```csharp
using Xunit;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Category;

public class NewServiceTests
{
    private readonly ITestOutputHelper _output;
    // ... другие поля (mocks, сервисы и т.д.)

    public NewServiceTests(ITestOutputHelper output)
    {
        _output = output;
        // ... существующая инициализация ...
    }

    [Fact(Timeout = 5000)]  // 🔴 высокий риск — WebSocket/сеть
    public async Task SomeAsyncMethod_ShouldDoSomething_Successfully()
    {
        _output.WriteLine($"=== Running: {nameof(SomeAsyncMethod_ShouldDoSomething_Successfully)} ===");

        // Arrange
        // Act
        // Assert
    }

    [Fact(Timeout = 5000)]  // 🟢 синхронный — без таймаута
    public void SomeSyncMethod_ShouldReturnCorrectResult()
    {
        // синхронные тесты не требуют таймаута
    }
}
```

### 5.4. Правила для новых тестов

| # | Правило | Пояснение |
|---|---------|-----------|
| 1 | **Все async-тесты** получают `[Fact(Timeout = ...)]` | Если тест зависнет, TimeoutException укажет имя теста |
| 2 | **Синхронные тесты** не получают таймаут | Они выполняются мгновенно и не могут "виснуть" в том же смысле |
| 3 | **ITestOutputHelper** добавляется в конструктор, а не через DI-контейнер | xUnit сам внедряет его при создании тестового класса |
| 4 | **Логгирование через `_output.WriteLine`** только для ключевых точек | Не захламлять вывод, только `"=== Running: {name} ==="` в начале async-теста + при необходимости точки входа/выхода |
| 5 | **Таймаут подбирается по риск-категории** | 5s для WebSocket/сеть, 10s для EF Core InMemory, без таймаута для синхронных |
| 6 | **Не использовать `Thread.Sleep` в тестах** | Заменять на `Task.Delay` с токеном отмены или await завершения |
| 7 | **Не передавать `_output` в тестируемый код** | `ITestOutputHelper` предназначен только для диагностики из теста, не из продакшен-кода |

### 5.5. Чек-лист при добавлении нового тестового файла

- [ ] Добавлен `using Xunit.Abstractions;`
- [ ] Добавлено поле `private readonly ITestOutputHelper _output;`
- [ ] Конструктор принимает `ITestOutputHelper output` и сохраняет в поле
- [ ] Каждый async-тест помечен `[Fact(Timeout = 5000)]` или `[Fact(Timeout = 10000)]`
- [ ] Каждый async-тест начинается с `_output.WriteLine($"=== Running: {nameof(...)} ===");`
- [ ] Синхронные тесты используют `[Fact(Timeout = 5000)]` без таймаута
- [ ] В `.csproj` добавлен `<Content Include="xunit.runner.json" CopyToOutputDirectory="PreserveNewest" />` (если ещё не добавлен)

### 5.6. Пример готового теста

```csharp
[Fact(Timeout = 10000)]
public async Task SaveTicksAsync_ShouldPersistTicksToDatabase()
{
    _output.WriteLine($"=== Running: {nameof(SaveTicksAsync_ShouldPersistTicksToDatabase)} ===");

    // Arrange
    var ticks = new List<RawTick> { /* ... */ };

    // Act
    await _repository.AddRangeAsync(ticks);
    await _repository.SaveChangesAsync();

    // Assert
    var saved = await _repository.GetAllAsync();
    Assert.Equal(ticks.Count, saved.Count());
}
```
