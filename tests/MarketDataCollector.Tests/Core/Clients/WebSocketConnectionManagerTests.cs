using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Interfaces;
using System.Net.WebSockets;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Core.Clients;

public class WebSocketConnectionManagerTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<ILogger<WebSocketConnectionManager>> _loggerMock;
    private readonly Mock<IClientWebSocket> _webSocketMock;

    public WebSocketConnectionManagerTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerMock = new Mock<ILogger<WebSocketConnectionManager>>();
        _webSocketMock = new Mock<IClientWebSocket>();
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullWebSocket_ThrowsArgumentNullException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<WebSocketConnectionManager>>();

        // Act & Assert
        var act = () => new WebSocketConnectionManager(loggerMock.Object, null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("webSocket");
    }

    [Fact(Timeout = 5000)]
    public void IsConnected_WhenClosed_ReturnsFalse()
    {
        // Arrange
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);

        // Act
        var result = manager.IsConnected;

        // Assert
        result.Should().BeFalse();
    }

    [Fact(Timeout = 5000)]
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

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects)} ===");
        // Arrange
        var newWebSocketMock = new Mock<IClientWebSocket>();
        newWebSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);

        var manager = new TestableWebSocketConnectionManager(
            _loggerMock.Object, _webSocketMock.Object, () => newWebSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.ConnectAsync(uri, cancellationToken);

        // Assert
        newWebSocketMock.Verify(ws => ws.ConnectAsync(uri, cancellationToken), Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WhenAlreadyConnected_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenAlreadyConnected_DoesNothing)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_WhenOpen_ClosesGracefully()
    {
        _output.WriteLine($"=== Running: {nameof(DisconnectAsync_WhenOpen_ClosesGracefully)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_WhenClosed_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(DisconnectAsync_WhenClosed_DoesNothing)} ===");
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.DisconnectAsync(cancellationToken);

        // Assert
        _webSocketMock.Verify(ws => ws.CloseAsync(It.IsAny<WebSocketCloseStatus>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_WhenCloseThrows_LogsWarning()
    {
        _output.WriteLine($"=== Running: {nameof(DisconnectAsync_WhenCloseThrows_LogsWarning)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task SendAsync_WhenNotOpen_ThrowsInvalidOperationException()
    {
        _output.WriteLine($"=== Running: {nameof(SendAsync_WhenNotOpen_ThrowsInvalidOperationException)} ===");
        // Arrange
        _webSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var manager = new WebSocketConnectionManager(_loggerMock.Object, _webSocketMock.Object);
        var cancellationToken = CancellationToken.None;

        // Act & Assert
        var act = async () => await manager.SendAsync("test message", cancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("WebSocket не подключён.");
    }

    [Fact(Timeout = 5000)]
    public async Task SendAsync_WhenOpen_SendsMessage()
    {
        _output.WriteLine($"=== Running: {nameof(SendAsync_WhenOpen_SendsMessage)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_WhenNotOpen_ThrowsInvalidOperationException()
    {
        _output.WriteLine($"=== Running: {nameof(ReceiveAsync_WhenNotOpen_ThrowsInvalidOperationException)} ===");
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

    [Fact(Timeout = 5000)]
    public async Task ReceiveAsync_WhenOpen_ReturnsResult()
    {
        _output.WriteLine($"=== Running: {nameof(ReceiveAsync_WhenOpen_ReturnsResult)} ===");
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

    [Fact(Timeout = 5000)]
    public void DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne()
    {
        // Arrange
        var newSocketMock = new Mock<IClientWebSocket>();
        newSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        var factoryCalled = false;
        var manager = new TestableWebSocketConnectionManager(
            _loggerMock.Object, _webSocketMock.Object, () =>
            {
                factoryCalled = true;
                return newSocketMock.Object;
            });

        // Act
        manager.DisposeCurrentSocket();

        // Assert
        _webSocketMock.Verify(ws => ws.Dispose(), Times.Once);
        factoryCalled.Should().BeTrue();
        manager.IsConnected.Should().BeFalse();
    }

    [Fact(Timeout = 5000)]
    public void StateChanged_Event_FiresOnConnect()
    {
        // Arrange
        var newSocketMock = new Mock<IClientWebSocket>();
        newSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        newSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var manager = new TestableWebSocketConnectionManager(
            _loggerMock.Object, _webSocketMock.Object, () => newSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");

        WebSocketState? receivedState = null;
        manager.StateChanged += (sender, state) => receivedState = state;

        // Act
        manager.ConnectAsync(uri, CancellationToken.None).GetAwaiter().GetResult();

        // Assert
        receivedState.Should().Be(WebSocketState.Open);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenCancelled_ThrowsOperationCanceledException)} ===");
        // Arrange
        var newSocketMock = new Mock<IClientWebSocket>();
        newSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Closed);
        newSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var manager = new TestableWebSocketConnectionManager(
            _loggerMock.Object, _webSocketMock.Object, () => newSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        // SemaphoreSlim.WaitAsync с предварительно отменённым токеном бросает TaskCanceledException,
        // который наследует OperationCanceledException — используем ThrowsAnyAsync
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            manager.ConnectAsync(uri, cts.Token));
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_ThreadSafety_ConcurrentCallsDoNotDeadlock()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_ThreadSafety_ConcurrentCallsDoNotDeadlock)} ===");
        // Arrange
        var newSocketMock = new Mock<IClientWebSocket>();
        newSocketMock.SetupGet(ws => ws.State).Returns(WebSocketState.Open);
        newSocketMock.Setup(ws => ws.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var manager = new TestableWebSocketConnectionManager(
            _loggerMock.Object, _webSocketMock.Object, () => newSocketMock.Object);
        var uri = new Uri("wss://example.com/ws");

        // Act - запускаем 3 параллельных ConnectAsync
        var tasks = Enumerable.Range(0, 3).Select(_ =>
            manager.ConnectAsync(uri, CancellationToken.None)).ToArray();

        // Assert - все задачи должны завершиться без deadlock
        var completedTask = await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(5000));
        completedTask.Should().NotBeSameAs(Task.Delay(5000), "ConnectAsync не должен вызывать deadlock при конкурентных вызовах");
        // Проверяем, что все задачи завершились успешно
        tasks.All(t => t.IsCompletedSuccessfully).Should().BeTrue();
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

    protected override IClientWebSocket CreateWebSocket()
    {
        return _socketFactory();
    }
}
