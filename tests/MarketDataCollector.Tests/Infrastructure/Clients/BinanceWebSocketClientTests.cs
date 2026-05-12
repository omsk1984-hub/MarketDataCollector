using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Globalization;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Infrastructure.Clients;

public class BinanceWebSocketClientTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<IWebSocketMessageReceiver> _messageReceiverMock;
    private readonly Mock<IReconnectStrategy> _reconnectStrategyMock;
    private readonly Mock<ILogger<BinanceWebSocketClient>> _loggerMock;
    private readonly Mock<IMarketDataProcessor> _dataProcessorMock;
    private readonly WebSocketClientOptions _defaultOptions;
    private readonly Uri _testUri;

    public BinanceWebSocketClientTests(ITestOutputHelper output)
    {
        _output = output;
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

    [Fact(Timeout = 5000)]
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

    [Fact(Timeout = 5000)]
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

    [Fact(Timeout = 5000)]
    public void GetWebSocketUri_ReturnsConstructorUri()
    {
        // Arrange & Act
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

    [Fact(Timeout = 5000)]
    public async Task SubscribeToTickerAsync_SendsCorrectJsonMessage()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeToTickerAsync_SendsCorrectJsonMessage)} ===");
        // Arrange
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

    [Fact(Timeout = 5000)]
    public async Task SubscribeToTickerAsync_SendsLowercaseSymbol()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeToTickerAsync_SendsLowercaseSymbol)} ===");
        // Arrange
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

    [Fact(Timeout = 5000)]
    public async Task ProcessMessageAsync_NonTradeMessage_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessMessageAsync_NonTradeMessage_DoesNothing)} ===");
        // Arrange
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

    [Fact(Timeout = 5000)]
    public async Task ProcessMessageAsync_MissingTicker_DoesNotCallDataProcessor()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessMessageAsync_MissingTicker_DoesNotCallDataProcessor)} ===");
        // Arrange
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

    [Fact(Timeout = 5000)]
    public async Task ProcessMessageAsync_InvalidJson_CallsOnErrorOccurred()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessMessageAsync_InvalidJson_CallsOnErrorOccurred)} ===");
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
        capturedException.Should().BeOfType<Newtonsoft.Json.JsonReaderException>();
    }

    [Fact(Timeout = 5000)]
    public async Task ProcessMessageAsync_MissingFields_UsesDefaultValues()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessMessageAsync_MissingFields_UsesDefaultValues)} ===");
        // Arrange
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

    [Theory(Timeout = 5000)]
    [InlineData("BTCUSDT", "1000.50", "0.5", "12345")]
    [InlineData("ETHUSDT", "2500.75", "1.25", "67890")]
    public async Task ProcessMessageAsync_ValidTradeMessage_CallsDataProcessorWithCorrectValues(
        string symbol, string priceStr, string volumeStr, string tradeId)
    {
        _output.WriteLine($"=== Running: {nameof(ProcessMessageAsync_ValidTradeMessage_CallsDataProcessorWithCorrectValues)} ===");
        // Arrange
        var jsonMessage = @"{
            ""e"": ""trade"",
            ""E"": 1609459200000,
            ""s"": """ + symbol + @""",
            ""t"": " + tradeId + @",
            ""p"": """ + priceStr + @""",
            ""q"": """ + volumeStr + @""",
            ""T"": 1609459200000
        }";

        var expectedPrice = decimal.Parse(priceStr, CultureInfo.InvariantCulture);
        var expectedVolume = decimal.Parse(volumeStr, CultureInfo.InvariantCulture);
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
            symbol,
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
