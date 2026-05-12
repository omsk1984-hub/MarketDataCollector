# План рефакторинга: WebSocketMessageReceiverTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. Нет проверки, что `processMessage` НЕ вызывается при превышении размера сообщения
2. `CancellationTokenSource(TimeSpan.FromSeconds(1))` создаёт ложноположительные результаты — тест может завершиться из-за таймаута, а не из-за проверяемого условия
3. Нет проверки продолжения цикла после ошибки ReceiveAsync
4. `cts.CancelAfter(50)` нестабилен

## Пошаговый план

### Шаг 1: Исправить тест `StartReceiveLoopAsync_ConnectionLost_BreaksLoop`

**Проблема:** `CancellationTokenSource(TimeSpan.FromSeconds(1))` завершит цикл через 1 секунду, даже если `IsConnected = false` не сработает. Тест может проходить ложно.

**Решение:** Убрать `CancellationTokenSource` с таймаутом. Использовать `Close`-сообщение для выхода из цикла, и проверять, что метод завершился из-за `IsConnected = false`.

```csharp
[Fact(Timeout = 5000)]
public async Task StartReceiveLoopAsync_ConnectionLost_BreaksLoop()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);

    var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    // Не используем CancellationTokenSource с таймаутом - используем предварительно отменённый
    using var cts = new CancellationTokenSource();
    cts.Cancel(); // Предотвращаем зависание, если IsConnected не сработает

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Warning,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Соединение разорвано")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 2: Исправить тест `StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage`

**Проблема:** Проверяет только лог, но не проверяет, что `processMessage` НЕ был вызван.

**Решение:** Добавить проверку, что `processMessage` не вызывался для превышающего сообщения, и что цикл продолжился.

```csharp
[Fact(Timeout = 5000)]
public async Task StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage()
{
    // Arrange
    var options = new WebSocketClientOptions
    {
        ReceiveBufferSize = 1024,
        MaxMessageSize = 100,
        ReconnectDelay = TimeSpan.FromSeconds(1),
        MaxReconnectDelay = TimeSpan.FromSeconds(60),
        MaxSubscribeRetries = 3
    };

    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(options),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);

    var processMessageCalled = false;
    var processMessage = new Func<string, Task>(async msg =>
    {
        processMessageCalled = true;
    });
    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    using var cts = new CancellationTokenSource();

    // Первое сообщение: превышает MaxMessageSize (200 > 100)
    var oversizedResult = new WebSocketReceiveResult(200, WebSocketMessageType.Text, false);
    // Оставшиеся фрагменты oversized сообщения
    var remainingFragment = new WebSocketReceiveResult(50, WebSocketMessageType.Text, false);
    var endFragment = new WebSocketReceiveResult(0, WebSocketMessageType.Text, true);
    // Второе сообщение: нормального размера (закрытие)
    var closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true);

    _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(oversizedResult)
        .ReturnsAsync(remainingFragment)
        .ReturnsAsync(endFragment)
        .ReturnsAsync(closeResult);

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

    // Assert
    // 1. processMessage НЕ вызывался для oversized сообщения
    processMessageCalled.Should().BeFalse();
    // 2. Лог содержит предупреждение о превышении размера
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Warning,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Сообщение превышает максимальный размер")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 3: Исправить тест `StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError`

**Проблема:** Не проверяет, что цикл продолжается после ошибки. `CancellationTokenSource(TimeSpan.FromSeconds(1))` может завершить тест до проверки.

**Решение:** Проверить, что после первой ошибки делается вторая попытка ReceiveAsync.

```csharp
[Fact(Timeout = 5000)]
public async Task StartReceiveLoopAsync_ReceiveThrowsException_CallsOnErrorAndContinues()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);

    var onErrorCalled = false;
    Exception? capturedException = null;
    var onError = new Action<Exception>(ex =>
    {
        onErrorCalled = true;
        capturedException = ex;
    });

    var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
    var onMessageReceived = new Action<string>(msg => { });

    using var cts = new CancellationTokenSource();

    // Первый ReceiveAsync выбрасывает исключение, второй возвращает Close
    var closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true);
    _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ThrowsAsync(new Exception("Test receive error"))
        .ReturnsAsync(closeResult);

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

    // Assert
    onErrorCalled.Should().BeTrue();
    capturedException.Should().NotBeNull();
    capturedException!.Message.Should().Be("Test receive error");
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.Is<Exception>(e => e.Message == "Test receive error"),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.AtLeastOnce);
    // Проверяем, что ReceiveAsync был вызван дважды (первый раз с ошибкой, второй раз Close)
    _connectionManagerMock.Verify(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
}
```

### Шаг 4: Исправить тест `StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop`

**Проблема:** `cts.CancelAfter(50)` нестабилен на медленных машинах.

**Решение:** Использовать предварительно отменённый токен.

```csharp
[Fact(Timeout = 5000)]
public async Task StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);

    var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    using var cts = new CancellationTokenSource();
    cts.Cancel(); // Предварительно отменённый токен — детерминированно

    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(new WebSocketReceiveResult(
            13,
            WebSocketMessageType.Text,
            true));

    // Act
    var task = receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

    // Assert - задача должна завершиться немедленно
    await Task.WhenAny(task, Task.Delay(1000));
    task.IsCompleted.Should().BeTrue();
    task.Exception.Should().BeNull();
}
```

### Шаг 5: Добавить тест на фрагментированные сообщения (опционально)

```csharp
[Fact(Timeout = 5000)]
public async Task StartReceiveLoopAsync_FragmentedMessage_AssemblesCorrectly()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);

    string? assembledMessage = null;
    var processMessage = new Func<string, Task>(async msg =>
    {
        assembledMessage = msg;
    });
    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    using var cts = new CancellationTokenSource();

    // Первый фрагмент: "Hello ", EndOfMessage=false
    var fragment1Bytes = System.Text.Encoding.UTF8.GetBytes("Hello ");
    var fragment1 = new WebSocketReceiveResult(fragment1Bytes.Length, WebSocketMessageType.Text, false);
    _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(fragment1);

    // Второй фрагмент: "World!", EndOfMessage=true
    var fragment2Bytes = System.Text.Encoding.UTF8.GetBytes("World!");
    var fragment2 = new WebSocketReceiveResult(fragment2Bytes.Length, WebSocketMessageType.Text, true);

    _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(fragment1)
        .ReturnsAsync(fragment2);

    // ... Close для выхода
    // ПРИМЕЧАНИЕ: Этот тест требует дополнительной настройки SetupSequence для корректной работы
    // Это зависит от того, как receiver читает данные из потока
}
```

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `tests/.../WebSocketMessageReceiverTests.cs` | Исправить `ConnectionLost_BreaksLoop` — убрать CancellationTokenSource с таймаутом |
| 2 | `tests/.../WebSocketMessageReceiverTests.cs` | Исправить `MessageExceedsMaxSize_SkipsMessage` — добавить проверку, что processMessage не вызван |
| 3 | `tests/.../WebSocketMessageReceiverTests.cs` | Исправить `ReceiveThrowsException_CallsOnError` — добавить проверку продолжения цикла |
| 4 | `tests/.../WebSocketMessageReceiverTests.cs` | Исправить `CancellationTokenRequested_StopsLoop` — использ. предварительно отменённый токен |
| 5 | `tests/.../WebSocketMessageReceiverTests.cs` | (Опционально) Добавить тест на фрагментированные сообщения |