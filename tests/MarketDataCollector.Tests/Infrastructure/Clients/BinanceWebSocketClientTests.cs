using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text.Json;
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
        
        testableClient.ErrorOccurred += (sender, ex) =>
        {
            errorOccurred = true;
            capturedException = ex;
        };

        var invalidJson = "not valid json";

        // Act
        await testableClient.TestProcessMessageAsync(invalidJson);

        // Assert
        errorOccurred.Should().BeTrue();
        capturedException.Should().NotBeNull();
        // Utf8JsonReader бросает JsonReaderException (наследник JsonException) при невалидном JSON
        capturedException.Should().BeAssignableTo<JsonException>();
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
    // Edge cases для ParseDecimalFromUtf8 (Variant A): целые, много знаков, отрицательные, "0.xxx".
    [InlineData("BTCUSDT", "100", "1", "11111")]
    [InlineData("BTCUSDT", "0.001", "0.000001", "22222")]
    [InlineData("BTCUSDT", "12345.6789", "999.9", "33333")]
    [InlineData("BTCUSDT", "-0.5", "-1", "44444")]
    [InlineData("BTCUSDT", "0", "0", "55555")]
    // P2: edge cases — leading zeros, scale=8 (DECIMAL(18,8)), границы диапазона, отрицательный ноль.
    [InlineData("BTCUSDT", "0.00000001", "0.1", "66666")]
    [InlineData("BTCUSDT", "9999999999.99999999", "9999999999.99999999", "77777")]
    [InlineData("BTCUSDT", "000123.4500", "0007", "88888")]
    [InlineData("BTCUSDT", "-0.0", "-0.00", "99999")]
    [InlineData("BTCUSDT", "1.", ".5", "10000")]
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

    [Theory(Timeout = 5000)]
    // P2: сверка с decimal.Parse(InvariantCulture) — гарантия, что long-накопление
    // даёт численно тот же результат, что и поразрядный decimal-алгоритм / стандартный парсер.
    [InlineData("0.001")]
    [InlineData("1000.50")]
    [InlineData("12345.6789")]
    [InlineData("0.00000001")]
    [InlineData("9999999999.99999999")]
    [InlineData("-12345.6789")]
    [InlineData("-0.5")]
    [InlineData("000123.4500")]
    [InlineData("-0.0")]
    [InlineData("0")]
    [InlineData("1.")]
    [InlineData(".5")]
    [InlineData("0.5")]
    public async Task ParseDecimalFromUtf8_MatchesDecimalParse(string valueStr)
    {
        var expected = decimal.Parse(valueStr, CultureInfo.InvariantCulture);

        var jsonMessage = @"{ ""e"": ""trade"", ""s"": ""BTCUSDT"", ""p"": """ + valueStr
            + @""", ""q"": ""1"", ""T"": 1609459200000 }";

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

        _dataProcessorMock.Verify(dp => dp.ProcessTickAsync(
            "BTCUSDT",
            expected,
            1m,
            DateTimeOffset.FromUnixTimeMilliseconds(1609459200000).UtcDateTime,
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
        var bytes = System.Text.Encoding.UTF8.GetBytes(message);
        return ProcessMessageAsync(new ReadOnlyMemory<byte>(bytes));
    }
}
