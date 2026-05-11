# План исправления тестов WebSocketMessageReceiverTests

## Обнаруженные проблемы

1. **Мок буфера**: В тестах используется конкретный экземпляр `ArraySegment<byte>`, но в реальном коде создается новый `ArraySegment<byte>` из массива пула. Это приводит к тому, что мок не срабатывает, потому что аргументы не совпадают.

2. **Бесконечные циклы**: Большинство тестов запускают цикл приема, но не обеспечивают его завершение. Это может привести к зависанию тестов.

3. **Некорректные проверки логов**: Проверки логов используют `Contains` для строк, что может быть хрупким.

4. **Отсутствие проверки вызова `processMessage`**: В тесте `StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage` не проверяется, что `processMessage` был вызван.

5. **Использование `Task.Delay` без гарантии завершения**: Тесты полагаются на задержки, что может привести к race condition.

## Исправления для каждого теста

### 1. Constructor_WithValidDependencies_SetsProperties
- Проблем нет, оставить как есть.

### 2. StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage
**Проблемы:**
- Мок настроен на конкретный буфер.
- Цикл не завершается.
- Не проверяется вызов `processMessage`.

**Исправления:**
- Заменить мок на `It.IsAny<ArraySegment<byte>>()`.
- Использовать `CancellationTokenSource` для отмены после одного сообщения.
- Добавить проверку, что `processMessage` был вызван с ожидаемым сообщением.

**Пример кода:**
```csharp
[Fact]
public async Task StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
    
    var receivedMessages = new List<string>();
    var processMessageCalled = false;
    
    var processMessage = async (string message) =>
    {
        receivedMessages.Add(message);
        processMessageCalled = true;
    };

    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    using var cts = new CancellationTokenSource();
    
    var expectedResult = new WebSocketReceiveResult(
        13,
        WebSocketMessageType.Text,
        true);
    
    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(expectedResult)
        .Callback(() => cts.CancelAfter(10)); // Отменяем после первого вызова

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

    // Assert
    processMessageCalled.Should().BeTrue();
    receivedMessages.Should().HaveCount(1);
    _connectionManagerMock.Verify(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
}
```

### 3. StartReceiveLoopAsync_ConnectionLost_BreaksLoop
**Проблемы:**
- Тест не ожидает завершения задачи.

**Исправления:**
- Использовать `await` для вызова `StartReceiveLoopAsync` с токеном отмены.
- Установить `IsConnected = false` перед вызовом.

**Пример кода:**
```csharp
[Fact]
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

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

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

### 4. StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage
**Проблемы:**
- После пропуска сообщения цикл продолжает работать.
- Мок настроен только на два вызова.

**Исправления:**
- Использовать `CancellationTokenSource` для отмены после пропуска.
- Настроить мок на возврат close сообщения после второго вызова.

**Пример кода:**
```csharp
[Fact]
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
    
    var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
    var onMessageReceived = new Action<string>(msg => { });
    var onError = new Action<Exception>(ex => { });

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    
    var firstResult = new WebSocketReceiveResult(200, WebSocketMessageType.Text, false);
    var secondResult = new WebSocketReceiveResult(0, WebSocketMessageType.Text, true);
    
    _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(firstResult)
        .ReturnsAsync(secondResult);

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);
    
    // Assert
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

### 5. StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError
**Проблемы:**
- Цикл продолжает работать после исключения.

**Исправления:**
- Использовать `CancellationTokenSource` для отмены после вызова `onError`.
- Настроить мок на бросок исключения только один раз.

**Пример кода:**
```csharp
[Fact]
public async Task StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
    
    var onErrorCalled = false;
    var onError = new Action<Exception>(ex =>
    {
        onErrorCalled = true;
        ex.Should().NotBeNull();
    });

    var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
    var onMessageReceived = new Action<string>(msg => { });

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    
    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ThrowsAsync(new Exception("Test receive error"));

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);
    
    // Assert
    onErrorCalled.Should().BeTrue();
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.Is<Exception>(e => e.Message == "Test receive error"),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.AtLeastOnce);
}
```

### 6. StartReceiveLoopAsync_ProcessMessageThrows_CallsOnError
**Проблемы:**
- Аналогично предыдущему.

**Исправления:**
- Использовать отмену токена после вызова `onError`.

**Пример кода:**
```csharp
[Fact]
public async Task StartReceiveLoopAsync_ProcessMessageThrows_CallsOnError()
{
    // Arrange
    var receiver = new WebSocketMessageReceiver(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object);

    _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
    
    var onErrorCalled = false;
    var onError = new Action<Exception>(ex =>
    {
        onErrorCalled = true;
        ex.Should().NotBeNull();
    });

    var processMessage = new Func<string, Task>(async msg =>
    {
        throw new Exception("Test process error");
    });
    var onMessageReceived = new Action<string>(msg => { });

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    
    var expectedResult = new WebSocketReceiveResult(
        13,
        WebSocketMessageType.Text,
        true);
    
    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(expectedResult);

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);
    
    // Assert
    onErrorCalled.Should().BeTrue();
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Ошибка при обработке сообщения")),
            It.Is<Exception>(e => e.Message == "Test process error"),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### 7. StartReceiveLoopAsync_ReceiveCloseMessage_BreaksLoop
**Проблемы:**
- Использование `Task.Delay` без гарантии завершения.

**Исправления:**
- Использовать `await` для вызова `StartReceiveLoopAsync`.

**Пример кода:**
```csharp
[Fact]
public async Task StartReceiveLoopAsync_ReceiveCloseMessage_BreaksLoop()
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

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
    
    var closeResult = new WebSocketReceiveResult(
        0,
        WebSocketMessageType.Close,
        true);
    
    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(closeResult);

    // Act
    await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);
    
    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Получено сообщение закрытия WebSocket")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### 8. StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop
**Проблемы:**
- Нет проверки завершения задачи.

**Исправления:**
- Добавить проверку, что задача завершилась без исключений.

**Пример кода:**
```csharp
[Fact]
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
    
    cts.CancelAfter(50);

    _connectionManagerMock.Setup(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(new WebSocketReceiveResult(
            13,
            WebSocketMessageType.Text,
            true));

    // Act
    var task = receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);
    
    // Ждем завершения задачи
    await Task.WhenAny(task, Task.Delay(1000));
    
    // Assert
    task.IsCompleted.Should().BeTrue();
    task.Exception.Should().BeNull();
}
```

### 9. StopReceiveLoop_LogsDebugMessage
- Проблем нет, оставить как есть.

## Общие изменения

1. **Замена конкретного буфера на `It.IsAny<ArraySegment<byte>>()`** во всех моках `ReceiveAsync`.
2. **Использование `CancellationTokenSource` с таймаутом** для гарантированного завершения циклов.
3. **Удаление `Task.Delay` из тестов** и использование `await` для вызова `StartReceiveLoopAsync`.
4. **Добавление проверок вызовов `processMessage`** где это необходимо.

## Следующие шаги

1. Переключиться в режим **code** для внесения изменений в файл тестов.
2. Запустить тесты для проверки исправлений.
3. При необходимости доработать тесты на основе результатов запуска.