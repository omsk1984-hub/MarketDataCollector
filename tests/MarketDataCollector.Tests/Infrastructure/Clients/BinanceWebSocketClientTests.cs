using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Globalization;

namespace MarketDataCollector.Tests.Infrastructure.Clients;

public class BinanceWebSocketClientTests
{
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<IWebSocketMessageReceiver> _messageReceiverMock;
    private readonly Mock<IReconnectStrategy> _reconnectStrategyMock;
    private readonly Mock<ILogger<BinanceWebSocketClient>> _loggerMock;
    private readonly Mock<IMarketDataProcessor> _dataProcessorMock;
    private readonly WebSocketClientOptions _defaultOptions;
    private readonly Uri _testUri;

    public BinanceWebSocketClientTests()
    {
        _connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        _messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        _reconnectStrategyMock = new Mock<IReconnectStrategy>();
        _loggerMock = new Mock<ILogger<BinanceWebSocketClient>>();
        _dataProcessorMock = new Mock<IMarketDataProcessor>();
        _defaultOptions = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = 3,
            DisposeTimeout = TimeSpan.FromSeconds(5)
        };
        _testUri = new Uri("wss://stream.binance.com:9443/ws/btcusdt@trade");
    }

    [Fact]
    public void Constructor_WithNullDataProcessor_ThrowsArgumentNullException()
    {
        // Arrange
        var connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        var messageReceiverMock = new Mock<IWebSocketMessageReceiver>();
        var reconnectStrategyMock = new Mock<IReconnectStrategy>();
        var loggerMock = new Mock<ILogger<BinanceWebSocketClient>>();

        // Act & Assert
        var act = () => new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            null!,
            connectionManagerMock.Object,
            messageReceiverMock.Object,
            reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            loggerMock.Object);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("dataProcessor");
    }

    [Fact]
    public void Constructor_SetsPropertiesCorrectly()
    {
        // Arrange & Act
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
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
    public void GetWebSocketUri_ReturnsConstructorUri()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        var uri = testableClient.TestGetWebSocketUri();

        // Assert
        uri.Should().Be(_testUri);
    }

    [Fact]
    public async Task SubscribeToTickerAsync_SendsCorrectJsonMessage()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var symbol = "BTCUSDT";
        var cancellationToken = CancellationToken.None;

        var expectedMessage = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}";

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestSubscribeToTickerAsync(symbol, cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.SendAsync(expectedMessage, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task SubscribeToTickerAsync_SendsLowercaseSymbol()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var symbol = "ETHUSDT";
        var cancellationToken = CancellationToken.None;

        var expectedMessage = "{\"method\":\"SUBSCRIBE\",\"params\":[\"ethusdt@trade\"],\"id\":1}";

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestSubscribeToTickerAsync(symbol, cancellationToken);

        // Assert
        _connectionManagerMock.Verify(cm => cm.SendAsync(expectedMessage, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task ProcessMessageAsync_ValidTradeMessage_CallsDataProcessor()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var jsonMessage = @"{
            ""e"": ""trade"",
            ""E"": 1234567890,
            ""s"": ""BTCUSDT"",
            ""t"": 12345,
            ""p"": ""1000.50"",
            ""q"": ""0.5"",
            ""T"": 1609459200000
        }";

        var expectedPrice = decimal.Parse("1000.50", CultureInfo.InvariantCulture);
        var expectedVolume = decimal.Parse("0.5", CultureInfo.InvariantCulture);
        var expectedTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1609459200000).UtcDateTime;

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(jsonMessage);

        // Assert
        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            "BTCUSDT",
            expectedPrice,
            expectedVolume,
            expectedTimestamp,
            "Binance"), Times.Once);
    }

    [Fact]
    public async Task ProcessMessageAsync_NonTradeMessage_DoesNothing()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var jsonMessage = @"{
            ""e"": ""24hrTicker"",
            ""s"": ""BTCUSDT""
        }";

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(jsonMessage);

        // Assert
        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            It.IsAny<string>(),
            It.IsAny<decimal>(),
            It.IsAny<decimal>(),
            It.IsAny<DateTime>(),
            It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ProcessMessageAsync_MissingTicker_DoesNotCallDataProcessor()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var jsonMessage = @"{
            ""e"": ""trade"",
            ""E"": 1234567890,
            ""t"": 12345,
            ""p"": ""1000.50"",
            ""q"": ""0.5"",
            ""T"": 1609459200000
        }";

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(jsonMessage);

        // Assert
        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            It.IsAny<string>(),
            It.IsAny<decimal>(),
            It.IsAny<decimal>(),
            It.IsAny<DateTime>(),
            It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ProcessMessageAsync_InvalidJson_CallsOnErrorOccurred()
    {
        // Arrange
        var errorOccurred = false;
        Exception? capturedException = null;
        
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        
        client.ErrorOccurred += (sender, ex) =>
        {
            errorOccurred = true;
            capturedException = ex;
        };

        var invalidJson = "not valid json";

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(invalidJson);

        // Assert
        errorOccurred.Should().BeTrue();
        capturedException.Should().NotBeNull();
        capturedException.Should().BeOfType<Exception>();
    }

    [Fact]
    public async Task ProcessMessageAsync_MissingFields_UsesDefaultValues()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var jsonMessage = @"{
            ""e"": ""trade"",
            ""E"": 1234567890,
            ""s"": ""BTCUSDT"",
            ""t"": 12345,
            ""T"": 1609459200000
        }";

        var expectedTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1609459200000).UtcDateTime;

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(jsonMessage);

        // Assert
        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            "BTCUSDT",
            0m, // default price
            0m, // default volume
            expectedTimestamp,
            "Binance"), Times.Once);
    }

    [Fact]
    public async Task ProcessMessageAsync_WithAllFields_CallsDataProcessorWithCorrectValues()
    {
        // Arrange
        var client = new BinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        var jsonMessage = @"{
            ""e"": ""trade"",
            ""E"": 1609459200000,
            ""s"": ""ETHUSDT"",
            ""t"": 67890,
            ""p"": ""2500.75"",
            ""q"": ""1.25"",
            ""T"": 1609459200000
        }";

        var expectedPrice = decimal.Parse("2500.75", CultureInfo.InvariantCulture);
        var expectedVolume = decimal.Parse("1.25", CultureInfo.InvariantCulture);
        var expectedTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1609459200000).UtcDateTime;

        // Act
        var testableClient = new TestableBinanceWebSocketClient(
            _testUri,
            "Binance",
            "BTCUSDT",
            _dataProcessorMock.Object,
            _connectionManagerMock.Object,
            _messageReceiverMock.Object,
            _reconnectStrategyMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object);
        await testableClient.TestProcessMessageAsync(jsonMessage);

        // Assert
        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            "ETHUSDT",
            expectedPrice,
            expectedVolume,
            expectedTimestamp,
            "Binance"), Times.Once);
    }
}

// Тестовый подкласс для тестирования protected методов
public class TestableBinanceWebSocketClient : BinanceWebSocketClient
{
    public TestableBinanceWebSocketClient(
        Uri webSocketUri,
        string exchangeName,
        string symbol,
        IMarketDataProcessor dataProcessor,
        IWebSocketConnectionManager connectionManager,
        IWebSocketMessageReceiver messageReceiver,
        IReconnectStrategy reconnectStrategy,
        IOptions<WebSocketClientOptions> options,
        ILogger<BinanceWebSocketClient> logger)
        : base(webSocketUri, exchangeName, symbol, dataProcessor, connectionManager, messageReceiver,
              reconnectStrategy, options, logger)
    {
    }

    public Uri TestGetWebSocketUri()
    {
        return GetWebSocketUri();
    }

    public Task TestSubscribeToTickerAsync(string symbol, CancellationToken cancellationToken)
    {
        return SubscribeToTickerAsync(symbol, cancellationToken);
    }

    public Task TestProcessMessageAsync(string message)
    {
        return ProcessMessageAsync(message);
    }
}
