# План рефакторинга: WebSocketConnectionManagerTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`
**Приоритет:** Критический (❌)

## Общая стратегия

Главная проблема — `TestableWebSocketConnectionManager` полностью переопределяет `ConnectAsync`, тестируя собственный код, а не реальную логику. Необходимо:
1. Удалить `TestableWebSocketConnectionManager`
2. Переписать тест `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects` так, чтобы он тестировал реальный `WebSocketConnectionManager.ConnectAsync`
3. Исправить остальные проблемные тесты

## Пошаговый план

### Шаг 1: Удалить `TestableWebSocketConnectionManager`

**Действие:** Удалить класс `TestableWebSocketConnectionManager` (строки 296-313).

```csharp
// Удалить полностью:
// public class TestableWebSocketConnectionManager : WebSocketConnectionManager
// {
//     private readonly Func<IClientWebSocket> _socketFactory;
//     ...
// }
```

### Шаг 2: Переписать тест `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects`

**Проблема:** Тест использует `TestableWebSocketConnectionManager`, который обходит реальную логику.

**Решение:** Тестировать реальный `WebSocketConnectionManager`. Реальная реализация [`ConnectAsync`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:38):
1. Захватывает `_connectLock` (SemaphoreSlim)
2. Проверяет `IsConnected` — если `true`, выходит
3. Создаёт новый `ClientWebSocketWrapper()` через `new`
4. Вызывает `ConnectAsync` на нём
5. Атомарно заменяет старый сокет через `Interlocked.Exchange`
6. Диспозит старый сокет

**Проблема:** `WebSocketConnectionManager` создаёт `new ClientWebSocketWrapper()` внутри метода, что нельзя замокать. Это архитектурное ограничение.

**Вариант А (рекомендуемый):** Сделать фабрику сокета injectable через конструктор или protected virtual метод.

**Изменения в исходном коде (`WebSocketConnectionManager.cs`):**

Добавить protected virtual метод для создания сокета:

```csharp
// В класс WebSocketConnectionManager добавить:
protected virtual IClientWebSocket CreateWebSocket()
{
    return new ClientWebSocketWrapper();
}
```

Изменить `ConnectAsync` (строка 50) с:
```csharp
var ws = new ClientWebSocketWrapper();
```
на:
```csharp
var ws = CreateWebSocket();
```

Аналогично изменить `DisposeCurrentSocket` (строка 114) с:
```csharp
var ws = Interlocked.Exchange(ref _webSocket, new ClientWebSocketWrapper());
```
на:
```csharp
var ws = Interlocked.Exchange(ref _webSocket, CreateWebSocket());
```

**Изменения в тесте:**

Создать тестовый наследник, который переопределяет только `CreateWebSocket`:

```csharp
// В файл тестов добавить:
public class TestableWebSocketConnectionManager : WebSocketConnectionManager
{
    private readonly Func<IClientWebSocket> _webSocketFactory;

    public TestableWebSocketConnectionManager(
        ILogger<WebSocketConnectionManager> logger,
        IClientWebSocket initialSocket,
        Func<IClientWebSocket> webSocketFactory)
        : base(logger, initialSocket)
    {
        _webSocketFactory = webSocketFactory;
    }

    protected override IClientWebSocket CreateWebSocket()
    {
        return _webSocketFactory();
    }
}
```

Переписать тест:

```csharp
[Fact(Timeout = 5000)]
public async Task ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects()
{
    _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects)} ===");
    // Arrange
    var newWebSocketMock = new Mock<IClientWebSocket>();
    newWebSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
    newWebSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
        .Returns(Task.CompletedTask);

    var manager = new TestableWebSocketConnectionManager(
        _loggerMock.Object,
        _webSocketMock.Object,
        () => newWebSocketMock.Object);

    var uri = new Uri("wss://example.com/ws");
    var cancellationToken = CancellationToken.None;

    // Act
    await manager.ConnectAsync(uri, cancellationToken);

    // Assert
    newWebSocketMock.Verify(ws => ws.ConnectAsync(uri, cancellationToken), Times.Once);
    manager.IsConnected.Should().BeTrue();
    // Проверить, что старый сокет был диспознут
    _webSocketMock.Verify(ws => ws.Dispose(), Times.Once);
}
```

### Шаг 3: Исправить тест `DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne`

**Проблема:** Проверка `manager.IsConnected.Should().BeFalse()` проходит по случайности — новый сокет имеет состояние `None`, а не `Closed`.

**Решение:** Проверять, что старый сокет диспознут, а не полагаться на `IsConnected`.

```csharp
[Fact(Timeout = 5000)]
public void DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne()
{
    // Arrange
    var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);

    // Act
    manager.DisposeCurrentSocket();

    // Assert
    _webSocketMock.Verify(ws => ws.Dispose(), Times.Once);
    // Новый сокет должен быть создан (IsConnected = false для None/Closed)
    manager.IsConnected.Should().BeFalse();
    // Дополнительно: проверить, что State не Closed (а None)
    manager.State.Should().Be(WebSocketState.None);
}
```

### Шаг 4: Исправить тест `StateChanged_Event_CanSubscribe`

**Проблема:** Тест не проверяет, что событие вызывается при изменении состояния.

**Решение:** Добавить тест, который проверяет вызов `StateChanged` после `ConnectAsync`.

```csharp
[Fact(Timeout = 5000)]
public async Task ConnectAsync_WhenConnected_RaisesStateChanged()
{
    // Arrange
    var newWebSocketMock = new Mock<IClientWebSocket>();
    newWebSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
    newWebSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
        .Returns(Task.CompletedTask);

    var manager = new TestableWebSocketConnectionManager(
        _loggerMock.Object,
        _webSocketMock.Object,
        () => newWebSocketMock.Object);

    var uri = new Uri("wss://example.com/ws");
    WebSocketState? capturedState = null;
    manager.StateChanged += (sender, state) => capturedState = state;

    // Act
    await manager.ConnectAsync(uri, CancellationToken.None);

    // Assert
    capturedState.Should().Be(WebSocketState.Open);
}
```

### Шаг 5: Исправить тест `ConnectAsync_WhenAlreadyConnected_DoesNothing`

**Проблема:** Не проверяется, что `_connectLock` освобождается корректно.

**Решение:** Добавить проверку, что после `ConnectAsync` можно вызвать другой метод без дедлока.

```csharp
[Fact(Timeout = 5000)]
public async Task ConnectAsync_WhenAlreadyConnected_DoesNothingAndReleasesLock()
{
    // Arrange
    _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
    var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
    var uri = new Uri("wss://example.com/ws");
    var cancellationToken = CancellationToken.None;

    // Act
    await manager.ConnectAsync(uri, cancellationToken);

    // Assert - проверяем, что блокировка освобождена (можем вызвать DisconnectAsync)
    await manager.DisconnectAsync(cancellationToken);
    _webSocketMock.Verify(ws => ws.CloseAsync(It.IsAny<WebSocketCloseStatus>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 6: Добавить тест на потокобезопасность

```csharp
[Fact(Timeout = 10000)]
public async Task ConnectAsync_ParallelCalls_OnlyOneConnects()
{
    // Arrange
    var newWebSocketMock = new Mock<IClientWebSocket>();
    newWebSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
    newWebSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
        .Returns(Task.CompletedTask);

    var manager = new TestableWebSocketConnectionManager(
        _loggerMock.Object,
        _webSocketMock.Object,
        () => newWebSocketMock.Object);

    var uri = new Uri("wss://example.com/ws");

    // Act - запускаем 3 параллельных ConnectAsync
    var tasks = Enumerable.Range(0, 3)
        .Select(_ => manager.ConnectAsync(uri, CancellationToken.None))
        .ToArray();

    await Task.WhenAll(tasks);

    // Assert - ConnectAsync должен быть вызван только один раз
    newWebSocketMock.Verify(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 7: Добавить тест на `ConnectAsync` с отменённым токеном

```csharp
[Fact(Timeout = 5000)]
public async Task ConnectAsync_WithCancelledToken_ThrowsOperationCanceledException()
{
    // Arrange
    var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
    var uri = new Uri("wss://example.com/ws");
    var cts = new CancellationTokenSource();
    cts.Cancel();

    // Act & Assert
    var act = async () => await manager.ConnectAsync(uri, cts.Token);
    await act.Should().ThrowAsync<OperationCanceledException>();
}
```

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs` | Добавить `protected virtual CreateWebSocket()` |
| 2 | `src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs` | Заменить `new ClientWebSocketWrapper()` на `CreateWebSocket()` в `ConnectAsync` и `DisposeCurrentSocket` |
| 3 | `tests/.../WebSocketConnectionManagerTests.cs` | Удалить старый `TestableWebSocketConnectionManager` |
| 4 | `tests/.../WebSocketConnectionManagerTests.cs` | Добавить новый `TestableWebSocketConnectionManager` с `CreateWebSocket` |
| 5 | `tests/.../WebSocketConnectionManagerTests.cs` | Переписать `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects` |
| 6 | `tests/.../WebSocketConnectionManagerTests.cs` | Исправить `DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne` |
| 7 | `tests/.../WebSocketConnectionManagerTests.cs` | Исправить `StateChanged_Event_CanSubscribe` |
| 8 | `tests/.../WebSocketConnectionManagerTests.cs` | Исправить `ConnectAsync_WhenAlreadyConnected_DoesNothing` |
| 9 | `tests/.../WebSocketConnectionManagerTests.cs` | Добавить тест на потокобезопасность |
| 10 | `tests/.../WebSocketConnectionManagerTests.cs` | Добавить тест на отменённый токен |