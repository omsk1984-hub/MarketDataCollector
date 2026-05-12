# План рефакторинга: BaseWebSocketClientTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. `.GetAwaiter().GetResult()` — синхронная блокировка, может вызвать дедлок
2. `SetSubscriptionManager_SetsManagerCorrectly` — использует `.GetAwaiter().GetResult()`
3. `StartAsync_StartsBackgroundRecoveryLoop` — не проверяет запуск цикла
4. `StopAsync_StopsBackgroundRecoveryLoop` — использует `Task.Delay(100)`
5. `DisposeAsync_DisposesResources` — `VerifyAdd` проверяет конструктор, а не `DisposeAsync`
6. `Dispose_DisposesResources` — синхронный `Dispose` может вызвать дедлок

## Пошаговый план

### Шаг 1: Исправить тест `SetSubscriptionManager_SetsManagerCorrectly`

**Проблема:** Использует `.GetAwaiter().GetResult()`, что может вызвать дедлок.

**Решение:** Использовать `async Task` и `await`.

```csharp
[Fact(Timeout = 5000)]
public async Task SetSubscriptionManager_SetsManagerCorrectly()
{
    // Arrange
    var subscriptionManagerMock = new Mock<ISubscriptionManager>();
    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);

    var client = new TestableWebSocketClient(
        _testUri,
        "Binance",
        "BTCUSDT",
        _connectionManagerMock.Object,
        _messageReceiverMock.Object,
        _reconnectStrategyMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    client.SetSubscriptionManager(subscriptionManagerMock.Object);

    // Act - используем await вместо GetAwaiter().GetResult()
    await client.ConnectAsync(CancellationToken.None);

    // Assert
    subscriptionManagerMock.Verify(sm => sm.SubscribeWithRetryAsync("BTCUSDT", CancellationToken.None), Times.Once);
}
```

### Шаг 2: Исправить тест `StartAsync_StartsBackgroundRecoveryLoop`

**Проблема:** Не проверяет, что `_connectionManager.ConnectAsync` вызывается внутри фонового цикла.

**Решение:** Настроить `_connectionManagerMock` и проверить, что `ConnectAsync` был вызван.

```csharp
[Fact(Timeout = 5000)]
public async Task StartAsync_StartsBackgroundRecoveryLoop()
{
    // Arrange
    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);
    _connectionManagerMock.Setup(cm => cm.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
        .Returns(Task.CompletedTask);

    var client = new TestableWebSocketClient(
        _testUri,
        "Binance",
        "BTCUSDT",
        _connectionManagerMock.Object,
        _messageReceiverMock.Object,
        _reconnectStrategyMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    using var cts = new CancellationTokenSource();

    // Act
    var task = client.StartAsync(cts.Token);

    // Assert
    task.Should().NotBeNull();
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Фоновый цикл восстановления запущен")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
    
    // Небольшая задержка для запуска цикла
    await Task.Delay(200);
    
    // Проверяем, что ConnectAsync был вызван внутри фонового цикла
    _connectionManagerMock.Verify(cm => cm.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);

    // Останавливаем
    cts.Cancel();
}
```

### Шаг 3: Исправить тест `StopAsync_StopsBackgroundRecoveryLoop`

**Проблема:** Использует `Task.Delay(100)`, не проверяет, что задача фонового цикла завершена.

**Решение:** Добавить проверку, что после `StopAsync` задача фонового цикла завершена.

```csharp
[Fact(Timeout = 5000)]
public async Task StopAsync_StopsBackgroundRecoveryLoop()
{
    // Arrange
    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);

    var client = new TestableWebSocketClient(
        _testUri,
        "Binance",
        "BTCUSDT",
        _connectionManagerMock.Object,
        _messageReceiverMock.Object,
        _reconnectStrategyMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    using var cts = new CancellationTokenSource();

    // Запускаем фоновый цикл
    await client.StartAsync(cts.Token);

    // Act
    await client.StopAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Фоновый цикл восстановления остановлен")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 4: Исправить тест `DisposeAsync_DisposesResources`

**Проблема:** `VerifyAdd` проверяет подписку в конструкторе, а не в `DisposeAsync`.

**Решение:** Разделить проверку подписки и отписки.

```csharp
[Fact(Timeout = 5000)]
public async Task DisposeAsync_DisposesResources()
{
    // Arrange
    var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
    var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
    var reconnectStrategyMock = new Mock<IReconnectStrategy>();
    var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

    var disposableConnectionManager = connectionManagerMock.As<IDisposable>();

    var client = new TestableWebSocketClient(
        _testUri,
        "Binance",
        "BTCUSDT",
        connectionManagerMock.Object,
        messageReceiverMock.Object,
        reconnectStrategyMock.Object,
        Options.Create(_defaultOptions),
        loggerMock.Object);

    // Act
    await client.DisposeAsync();

    // Assert
    messageReceiverMock.Verify(mr => mr.StopReceiveLoop(), Times.Once);
    // Проверяем только отписку от события (подписка была в конструкторе)
    connectionManagerMock.VerifyRemove(cm => cm.StateChanged -= It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
    disposableConnectionManager.Verify(d => d.Dispose(), Times.Once);
}
```

### Шаг 5: Исправить тест `Dispose_DisposesResources`

**Проблема:** Синхронный `Dispose` вызывает `StopAsync(...).Wait(_options.DisposeTimeout)`, что может вызвать дедлок.

**Решение:** Добавить проверку, что `Dispose` завершается в течение таймаута.

```csharp
[Fact(Timeout = 5000)]
public void Dispose_DisposesResources()
{
    // Arrange
    var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
    var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
    var reconnectStrategyMock = new Mock<IReconnectStrategy>();
    var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

    var disposableConnectionManager = connectionManagerMock.As<IDisposable>();

    var client = new TestableWebSocketClient(
        _testUri,
        "Binance",
        "BTCUSDT",
        connectionManagerMock.Object,
        messageReceiverMock.Object,
        reconnectStrategyMock.Object,
        Options.Create(_defaultOptions),
        loggerMock.Object);

    // Act
    client.Dispose();

    // Assert
    messageReceiverMock.Verify(mr => mr.StopReceiveLoop(), Times.Once);
    connectionManagerMock.VerifyRemove(cm => cm.StateChanged -= It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
    disposableConnectionManager.Verify(d => d.Dispose(), Times.Once);
}
```

### Шаг 6: Заменить `.GetAwaiter().GetResult()` на `await` во всех остальных тестах

**Действие:** Найти все вхождения `.GetAwaiter().GetResult()` и `.Result` в файле и заменить на `await`.

Текущие вхождения:
- Строка 242: `client.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult()` (исправлено в Шаге 1)

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `tests/.../BaseWebSocketClientTests.cs` | Исправить `SetSubscriptionManager_SetsManagerCorrectly` — `await` вместо `.GetAwaiter().GetResult()` |
| 2 | `tests/.../BaseWebSocketClientTests.cs` | Исправить `StartAsync_StartsBackgroundRecoveryLoop` — добавить проверку вызова ConnectAsync |
| 3 | `tests/.../BaseWebSocketClientTests.cs` | Исправить `StopAsync_StopsBackgroundRecoveryLoop` — убрать Task.Delay |
| 4 | `tests/.../BaseWebSocketClientTests.cs` | Исправить `DisposeAsync_DisposesResources` — убрать VerifyAdd |
| 5 | `tests/.../BaseWebSocketClientTests.cs` | Исправить `Dispose_DisposesResources` — оставить как есть, но убедиться в таймауте |
| 6 | `tests/.../BaseWebSocketClientTests.cs` | Проверить отсутствие других `.GetAwaiter().GetResult()` |