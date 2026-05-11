using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using System.Net.WebSockets;

namespace MarketDataCollector.Tests.Core.Clients;

public class WebSocketMessageReceiverTests
{
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<ILogger<WebSocketMessageReceiver>> _loggerMock;
    private readonly WebSocketClientOptions _defaultOptions;

    public WebSocketMessageReceiverTests()
    {
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

    [Fact]
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
        var secondResult = new WebSocketReceiveResult(0, WebSocketMessageType.Text, true); // EndOfMessage
        
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
        
        // Отменяем токен сразу после начала
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

    [Fact]
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
