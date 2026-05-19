using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using System.Net.WebSockets;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Core.Clients;

public class WebSocketMessageReceiverTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<ILogger<WebSocketMessageReceiver>> _loggerMock;
    private readonly WebSocketClientOptions _defaultOptions;

    public WebSocketMessageReceiverTests(ITestOutputHelper output)
    {
        _output = output;
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        
        _connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        _loggerMock = new Mock<ILogger<WebSocketMessageReceiver>>();
        _defaultOptions = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = 3
        };
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidDependencies_SetsProperties()
    {   
        // Arrange & Act
        var receiver = new WebSocketMessageReceiver(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Assert
        receiver.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage)} ===");
        // Arrange
        var receiver = new WebSocketMessageReceiver(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
        
        var receivedMessages = new List<string>();
        var processMessageCalled = false;
        
        var processMessage = (string message) =>
        {
            receivedMessages.Add(message);
            processMessageCalled = true;
            return Task.CompletedTask;
        };

        var onMessageReceived = new Action<string>(msg => { });
        var onError = new Action<Exception>(ex => { });

        using var cts = new CancellationTokenSource();
        
        var expectedResult = new WebSocketReceiveResult(
            13,
            WebSocketMessageType.Text,
            true);
        
        var closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true);
        _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult)
            .ReturnsAsync(closeResult); // Close-сообщение для выхода из цикла

        // Act
        await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, cts.Token);

        // Assert
        processMessageCalled.Should().BeTrue();
        receivedMessages.Should().HaveCount(1);
        _connectionManagerMock.Verify(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_ConnectionLost_BreaksLoop()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_ConnectionLost_BreaksLoop)} ===");
        // Arrange
        var receiver = new WebSocketMessageReceiver(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);
        
        var processMessage = new Func<string, Task>(msg => Task.CompletedTask);
        var onMessageReceived = new Action<string>(msg => { });
        var onError = new Action<Exception>(ex => { });

        // Используем неотменённый токен — цикл выйдет по проверке IsConnected
        // а не по признаку отмены, что позволяет проверить логику обработки разрыва соединения

        // Act
        await receiver.StartReceiveLoopAsync(processMessage, onMessageReceived, onError, CancellationToken.None);
        
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

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage)} ===");
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
        var processMessage = new Func<string, Task>(msg =>
        {
            processMessageCalled = true;
            return Task.CompletedTask;
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

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_ReceiveThrowsException_CallsOnErrorAndContinues()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_ReceiveThrowsException_CallsOnErrorAndContinues)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_ProcessMessageThrows_CallsOnError()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_ProcessMessageThrows_CallsOnError)} ===");
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

        var processMessage = new Func<string, Task>(msg =>
        {
            throw new Exception("Test process error");
        });
        var onMessageReceived = new Action<string>(msg => { });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        
        var expectedResult = new WebSocketReceiveResult(
            13,
            WebSocketMessageType.Text,
            true);
        
        var closeResult = new WebSocketReceiveResult(0, WebSocketMessageType.Close, true);
        _connectionManagerMock.SetupSequence(cm => cm.ReceiveAsync(It.IsAny<ArraySegment<byte>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult)
            .ReturnsAsync(closeResult); // Close-сообщение для выхода из цикла

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

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_ReceiveCloseMessage_BreaksLoop()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_ReceiveCloseMessage_BreaksLoop)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop()
    {
        _output.WriteLine($"=== Running: {nameof(StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop)} ===");
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

    [Fact(Timeout = 5000)]
    public void StopReceiveLoop_LogsDebugMessage()
    {
        // Arrange
        var receiver = new WebSocketMessageReceiver(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        receiver.StopReceiveLoop();

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Остановка цикла приёма сообщений запрошена")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }
}
