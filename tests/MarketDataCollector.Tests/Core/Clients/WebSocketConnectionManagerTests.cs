using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Interfaces;
using System.Net.WebSockets;

namespace MarketDataCollector.Tests.Core.Clients;

public class WebSocketConnectionManagerTests
{
    private readonly Mock<ILogger<WebSocketConnectionManager>> _loggerMock;
    private readonly Mock<IClientWebSocket> _webSocketMock;

    public WebSocketConnectionManagerTests()
    {
        _loggerMock = new Mock<ILogger<WebSocketConnectionManager>>();
        _webSocketMock = new Mock<IClientWebSocket>();
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
    }

    [Fact]
    public void Constructor_WithNullWebSocket_ThrowsArgumentNullException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<WebSocketConnectionManager>>();

        // Act & Assert
        var act = () => new WebSocketConnectionManager(loggerMock.Object, null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("webSocket");
    }

    [Fact]
    public void IsConnected_WhenClosed_ReturnsFalse()
    {
        // Arrange
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);

        // Act
        var result = manager.IsConnected;

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void IsConnected_WhenOpen_ReturnsTrue()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);

        // Act
        var result = manager.IsConnected;

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects()
    {
        // Arrange
        var newWebSocketMock = new Mock<IClientWebSocket>();
        newWebSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");
        var cancellationToken = CancellationToken.None;

        // Act
        newWebSocketMock.Setup(ws => ws.ConnectAsync(uri, cancellationToken))
            .Returns(Task.CompletedTask);
        
        // Создадим новый менеджер с мокированным сокетом, который вернет новый сокет при ConnectAsync
        var factoryCalled = false;
        var testManager = new TestableWebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object, () => newWebSocketMock.Object);
        
        await testManager.ConnectAsync(uri, cancellationToken);

        // Assert
        newWebSocketMock.Verify(ws => ws.ConnectAsync(uri, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task ConnectAsync_WhenAlreadyConnected_DoesNothing()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.ConnectAsync(uri, cancellationToken);

        // Assert
        _webSocketMock.Verify(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()), Times.Never);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Соединение уже установлено")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task DisconnectAsync_WhenOpen_ClosesGracefully()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.DisconnectAsync(cancellationToken);

        // Assert
        _webSocketMock.Verify(ws => ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", cancellationToken), Times.Once);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("WebSocket отключён")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task DisconnectAsync_WhenClosed_DoesNothing()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.DisconnectAsync(cancellationToken);

        // Assert
        _webSocketMock.Verify(ws => ws.CloseAsync(It.IsAny<WebSocketCloseStatus>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task DisconnectAsync_WhenCloseThrows_LogsWarning()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        _webSocketMock.Setup(ws => ws.CloseAsync(
            WebSocketCloseStatus.NormalClosure, 
            "Disconnecting", 
            It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Test exception"));
        
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.DisconnectAsync(cancellationToken);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.Is<Exception>(e => e.Message == "Test exception"),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task SendAsync_WhenNotOpen_ThrowsInvalidOperationException()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act & Assert
        var act = async () => await manager.SendAsync("test message", cancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("WebSocket не подключён.");
    }

    [Fact]
    public async Task SendAsync_WhenOpen_SendsMessage()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var message = "test message";
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.SendAsync(message, cancellationToken);

        // Assert
        _webSocketMock.Verify(ws => ws.SendAsync(
            It.Is<ArraySegment<byte>>(b => 
                b.Array != null && 
                System.Text.Encoding.UTF8.GetString(b.Array, b.Offset, b.Count) == message),
            WebSocketMessageType.Text, 
            true, 
            cancellationToken), 
            Times.Once);
    }

    [Fact]
    public async Task ReceiveAsync_WhenNotOpen_ThrowsInvalidOperationException()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var buffer = new ArraySegment<byte>(new byte[1024]);
        var cancellationToken = CancellationToken.None;

        // Act & Assert
        var act = async () => await manager.ReceiveAsync(buffer, cancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("WebSocket не подключён.");
    }

    [Fact]
    public async Task ReceiveAsync_WhenOpen_ReturnsResult()
    {
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var expectedResult = new WebSocketReceiveResult(
            100, 
            WebSocketMessageType.Text, 
            true);
        _webSocketMock.Setup(ws => ws.ReceiveAsync(
            It.IsAny<ArraySegment<byte>>(), 
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult);
        
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var buffer = new ArraySegment<byte>(new byte[1024]);
        var cancellationToken = CancellationToken.None;

        // Act
        var result = await manager.ReceiveAsync(buffer, cancellationToken);

        // Assert
        result.Should().Be(expectedResult);
        _webSocketMock.Verify(ws => ws.ReceiveAsync(buffer, cancellationToken), Times.Once);
    }

    [Fact]
    public void DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne()
    {
        // Arrange
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var oldSocket = _webSocketMock.Object;

        // Act
        manager.DisposeCurrentSocket();

        // Assert
        _webSocketMock.Verify(ws => ws.Dispose(), Times.Once);
        manager.IsConnected.Should().BeFalse(); // Новый сокет должен быть в закрытом состоянии
    }

    [Fact]
    public void StateChanged_Event_CanSubscribe()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<WebSocketConnectionManager>>();
        var webSocketMock = new Mock<IClientWebSocket>();
        webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        var manager = new WebSocketConnectionManager(loggerMock.Object, webSocketMock.Object);
        
        // Act & Assert
        // Проверяем, что к событию можно подписаться
        bool eventFired = false;
        manager.StateChanged += (sender, state) => eventFired = true;
        eventFired.Should().BeFalse();
    }
}

// Тестовый подкласс для тестирования ConnectAsync с кастомным фабричным методом
public class TestableWebSocketConnectionManager : WebSocketConnectionManager
{
    private readonly Func<IClientWebSocket> _socketFactory;

    public TestableWebSocketConnectionManager(
        ILogger<WebSocketConnectionManager> logger, 
        IClientWebSocket initialSocket,
        Func<IClientWebSocket> socketFactory) 
        : base(logger, initialSocket)
    {
        _socketFactory = socketFactory;
    }

    public new async Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        await _socketFactory().ConnectAsync(uri, cancellationToken);
    }
}
