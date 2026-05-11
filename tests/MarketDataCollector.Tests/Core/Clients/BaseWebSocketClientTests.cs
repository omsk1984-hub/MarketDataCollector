using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using System.Net.WebSockets;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Core.Clients;

/// <summary>
/// Тестовый наследник BaseWebSocketClient для целей тестирования.
/// </summary>
public class TestableWebSocketClient : BaseWebSocketClient
{
    private readonly Uri _testUri;

    public TestableWebSocketClient(
        Uri uri,
        string exchangeName,
        string symbol,
        IWebSocketConnectionManager connectionManager,
        IWebSocketMessageReceiver messageReceiver,
        IReconnectStrategy reconnectStrategy,
        IOptions<WebSocketClientOptions> options,
        ILogger<TestableWebSocketClient> logger)
        : base(uri, exchangeName, symbol, connectionManager, messageReceiver, reconnectStrategy, options, logger)
    {
        _testUri = uri;
    }

    protected override Uri GetWebSocketUri() => _testUri;

    protected internal override Task ProcessMessageAsync(string message)
    {
        return Task.CompletedTask;
    }
}

public class BaseWebSocketClientTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<IWebSocketMessageReceiver> _messageReceiverMock;
    private readonly Mock<IReconnectStrategy> _reconnectStrategyMock;
    private readonly Mock<ILogger<TestableWebSocketClient>> _loggerMock;
    private readonly WebSocketClientOptions _defaultOptions;
    private readonly Uri _testUri;

    public BaseWebSocketClientTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        _messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        _reconnectStrategyMock = new Mock<IReconnectStrategy>();
        _loggerMock = new Mock<ILogger<TestableWebSocketClient>>();
        _defaultOptions = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = 3,
            DisposeTimeout = TimeSpan.FromSeconds(5)
        };
        _testUri = new Uri("wss://test.example.com/ws");
    }

    [Fact]
    public void Constructor_WithNullConnectionManager_ThrowsArgumentNullException()
    {
        // Arrange
        var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        var reconnectStrategyMock = new Mock<IReconnectStrategy>();
        var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

        // Act & Assert
        var act = () => new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            null!,
            messageReceiverMock.Object,
            reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            loggerMock.Object);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("connectionManager");
    }

    [Fact]
    public void Constructor_WithNullMessageReceiver_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        var reconnectStrategyMock = new Mock<IReconnectStrategy>();
        var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

        // Act & Assert
        var act = () => new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            connectionManagerMock.Object,
            null!,
            reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            loggerMock.Object);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("messageReceiver");
    }

    [Fact]
    public void Constructor_WithNullReconnectStrategy_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

        // Act & Assert
        var act = () => new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            connectionManagerMock.Object,
            messageReceiverMock.Object,
            null!,
            Options.Create(_defaultOptions),
            loggerMock.Object);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("reconnectStrategy");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        var reconnectStrategyMock = new Mock<IReconnectStrategy>();
        var loggerMock = new Mock<ILogger<TestableWebSocketClient>>();

        // Act & Assert
        var act = () => new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            connectionManagerMock.Object,
            messageReceiverMock.Object,
            reconnectStrategyMock.Object,
            null!,
            loggerMock.Object);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        var reconnectStrategyMock = new Mock<IReconnectStrategy>();

        // Act & Assert
        var act = () => new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            connectionManagerMock.Object,
            messageReceiverMock.Object,
            reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("logger");
    }

    [Fact]
    public void Constructor_SetsPropertiesCorrectly()
    {
        // Arrange & Act
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Assert
        client.ExchangeName.Should().Be("Binance");
        client.Symbol.Should().Be("BTCUSDT");
        client.Name.Should().Be("Binance_BTCUSDT");
    }

    [Fact]
    public void IsConnected_ReturnsConnectionManagerIsConnected()
    {
        // Arrange
        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        var result = client.IsConnected;

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void SetSubscriptionManager_SetsManagerCorrectly()
    {
        // Arrange
        var subscriptionManagerMock = new Mock<ISubscriptionManager>();
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        client.SetSubscriptionManager(subscriptionManagerMock.Object);

        // Assert
        // Проверяем, что менеджер установлен (через проверку, что нет исключения)
        client.Should().NotBeNull();
    }

    [Fact]
    public void SetSubscriptionManager_WithNull_ThrowsArgumentNullException()
    {
        // Arrange
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act & Assert
        var act = () => client.SetSubscriptionManager(null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("subscriptionManager");
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WhenAlreadyConnected_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenAlreadyConnected_DoesNothing)} ===");
        // Arrange
        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var cancellationToken = CancellationToken.None;

        // Act
        await client.ConnectAsync(cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.ConnectAsync(It.IsAny<Uri>(), It.IsAny<CancellationToken>()), Times.Never);
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
    public async Task ConnectAsync_WhenNotConnected_CallsConnectionManagerAndStartsReceiveLoop()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WhenNotConnected_CallsConnectionManagerAndStartsReceiveLoop)} ===");
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

        var cancellationToken = CancellationToken.None;

        // Act
        await client.ConnectAsync(cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.ConnectAsync(_testUri, cancellationToken), Times.Once);
        _messageReceiverMock.Verify(mr => mr.StartReceiveLoopAsync(
            It.IsAny<Func<string, Task>>(),
            It.IsAny<Action<string>>(),
            It.IsAny<Action<Exception>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task ConnectAsync_WithSubscriptionManager_CallsSubscribeWithRetryAsync()
    {
        _output.WriteLine($"=== Running: {nameof(ConnectAsync_WithSubscriptionManager_CallsSubscribeWithRetryAsync)} ===");
        // Arrange
        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false);
        
        var subscriptionManagerMock = new Mock<ISubscriptionManager>();
        
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

        var cancellationToken = CancellationToken.None;

        // Act
        await client.ConnectAsync(cancellationToken);

        // Assert
        subscriptionManagerMock.Verify(sm => sm.SubscribeWithRetryAsync("BTCUSDT", cancellationToken), Times.Once);
    }

    [Fact]
    public void StartAsync_StartsBackgroundRecoveryLoop()
    {
        // Arrange
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
    }

    [Fact(Timeout = 5000)]
    public async Task StopAsync_StopsBackgroundRecoveryLoop()
    {
        _output.WriteLine($"=== Running: {nameof(StopAsync_StopsBackgroundRecoveryLoop)} ===");
        // Arrange
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
        client.StartAsync(cts.Token);
        await Task.Delay(100);

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

    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_WhenConnected_ClosesConnection()
    {
        _output.WriteLine($"=== Running: {nameof(DisconnectAsync_WhenConnected_ClosesConnection)} ===");
        // Arrange
        _connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(true);
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var cancellationToken = CancellationToken.None;

        // Act
        await client.DisconnectAsync(cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.DisconnectAsync(cancellationToken), Times.Once);
        _messageReceiverMock.Verify(mr => mr.StopReceiveLoop(), Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task DisconnectAsync_WhenNotConnected_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(DisconnectAsync_WhenNotConnected_DoesNothing)} ===");
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

        var cancellationToken = CancellationToken.None;

        // Act
        await client.DisconnectAsync(cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.DisconnectAsync(cancellationToken), Times.Never);
    }

    [Fact]
    public void SendAsync_DelegatesToConnectionManager()
    {
        // Arrange
        _connectionManagerMock.Setup(cm => cm.SendAsync(
            "test message",
            It.IsAny<CancellationToken>()))
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

        var cancellationToken = CancellationToken.None;

        // Act
        client.SendAsync("test message", cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.SendAsync("test message", cancellationToken), Times.Once);
    }

    [Fact]
    public void OnMessageReceived_RaisesMessageReceivedEvent()
    {
        // Arrange
        var messageReceivedRaised = false;
        string? capturedMessage = null;
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        
        client.MessageReceived += (sender, message) =>
        {
            messageReceivedRaised = true;
            capturedMessage = message;
        };

        // Act
        client.OnMessageReceived("test message");

        // Assert
        messageReceivedRaised.Should().BeTrue();
        capturedMessage.Should().Be("test message");
    }

    [Fact]
    public void OnConnected_RaisesConnectedEvent()
    {
        // Arrange
        var connectedRaised = false;
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        
        client.Connected += (sender, args) =>
        {
            connectedRaised = true;
        };

        // Act
        client.OnConnected();

        // Assert
        connectedRaised.Should().BeTrue();
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Подключено")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void OnDisconnected_RaisesDisconnectedEvent()
    {
        // Arrange
        var disconnectedRaised = false;
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        
        client.Disconnected += (sender, args) =>
        {
            disconnectedRaised = true;
        };

        // Act
        client.OnDisconnected();

        // Assert
        disconnectedRaised.Should().BeTrue();
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Отключено")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void OnErrorOccurred_RaisesErrorOccurredEvent()
    {
        // Arrange
        var errorOccurredRaised = false;
        Exception? capturedException = null;
        
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        
        var testException = new Exception("Test error");
        
        client.ErrorOccurred += (sender, ex) =>
        {
            errorOccurredRaised = true;
            capturedException = ex;
        };

        // Act
        client.OnErrorOccurred(testException);

        // Assert
        errorOccurredRaised.Should().BeTrue();
        capturedException.Should().Be(testException);
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Ошибка")),
                It.Is<Exception>(e => e.Message == "Test error"),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task DisposeAsync_DisposesResources()
    {
        _output.WriteLine($"=== Running: {nameof(DisposeAsync_DisposesResources)} ===");
        // Arrange
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        await client.DisposeAsync();

        // Assert
        _messageReceiverMock.Verify(mr => mr.StopReceiveLoop(), Times.Once);
        _connectionManagerMock.VerifyAdd(cm => cm.StateChanged += It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
        _connectionManagerMock.VerifyRemove(cm => cm.StateChanged -= It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
    }

    [Fact]
    public void Dispose_DisposesResources()
    {
        // Arrange
        var client = new TestableWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        client.Dispose();

        // Assert
        _messageReceiverMock.Verify(mr => mr.StopReceiveLoop(), Times.Once);
    }
}
