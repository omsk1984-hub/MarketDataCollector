using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;
using MarketDataCollector.Infrastructure.Factories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Linq;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Infrastructure.Factories;

public class WebSocketClientFactoryTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IMarketDataProcessor> _dataProcessorMock;
    private readonly Mock<IMonitoringService> _monitoringServiceMock;
    private readonly Mock<IOptions<ExchangeOptions>> _exchangeOptionsMock;
    private readonly Mock<IOptions<WebSocketClientOptions>> _wsOptionsMock;
    private readonly ILoggerFactory _loggerFactory;
    private readonly WebSocketClientFactory _factory;

    public WebSocketClientFactoryTests(ITestOutputHelper output)
    {
        _output = output;
        _dataProcessorMock = new Mock<IMarketDataProcessor>();
        _monitoringServiceMock = new Mock<IMonitoringService>();
        _exchangeOptionsMock = new Mock<IOptions<ExchangeOptions>>();
        _wsOptionsMock = new Mock<IOptions<WebSocketClientOptions>>();
        _loggerFactory = LoggerFactory.Create(builder => { });

        // Setup options
        _exchangeOptionsMock.Setup(x => x.Value).Returns(new ExchangeOptions
        {
            Exchanges = new List<ExchangeConfig>
            {
                new ExchangeConfig
                {
                    ExchangeName = "Binance",
                    WebSocketUrl = "wss://stream.binance.com:9443/ws/{symbol}@trade",
                    Symbol = "BTCUSDT"
                }
            },
            Readers = new List<ReaderConfig>
            {
                new ReaderConfig
                {
                    ExchangeName = "Binance",
                    Symbol = "BTCUSDT"
                }
            }
        });

        _wsOptionsMock.Setup(x => x.Value).Returns(new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxInternalReconnectAttempts = 5,
            MaxSubscribeRetries = 3,
            ReceiveBufferSize = 4096,
            MaxMessageSize = 1_048_576,
            DisposeTimeout = TimeSpan.FromSeconds(5)
        });

        _factory = new WebSocketClientFactory(
            _dataProcessorMock.Object,
            _monitoringServiceMock.Object,
            _exchangeOptionsMock.Object,
            _wsOptionsMock.Object,
            _loggerFactory);
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidParameters_CreatesFactory()
    {
        // Arrange & Act
        var factory = new WebSocketClientFactory(
            _dataProcessorMock.Object,
            _monitoringServiceMock.Object,
            _exchangeOptionsMock.Object,
            _wsOptionsMock.Object,
            _loggerFactory);

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullDataProcessor_ThrowsArgumentNullException()
    {
        // Arrange
        IMarketDataProcessor? nullProcessor = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new WebSocketClientFactory(
            nullProcessor,
            _monitoringServiceMock.Object,
            _exchangeOptionsMock.Object,
            _wsOptionsMock.Object,
            _loggerFactory));
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullMonitoringService_ThrowsArgumentNullException()
    {
        // Arrange
        IMonitoringService? nullMonitoring = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new WebSocketClientFactory(
            _dataProcessorMock.Object,
            nullMonitoring,
            _exchangeOptionsMock.Object,
            _wsOptionsMock.Object,
            _loggerFactory));
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullExchangeOptions_ThrowsArgumentNullException()
    {
        // Arrange
        IOptions<ExchangeOptions>? nullOptions = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new WebSocketClientFactory(
            _dataProcessorMock.Object,
            _monitoringServiceMock.Object,
            nullOptions,
            _wsOptionsMock.Object,
            _loggerFactory));
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullWsOptions_ThrowsArgumentNullException()
    {
        // Arrange
        IOptions<WebSocketClientOptions>? nullOptions = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new WebSocketClientFactory(
            _dataProcessorMock.Object,
            _monitoringServiceMock.Object,
            _exchangeOptionsMock.Object,
            nullOptions,
            _loggerFactory));
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_WithValidParameters_CreatesClient()
    {
        // Arrange & Act
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<BinanceWebSocketClient>();
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_CreatesClientWithCorrectSymbol()
    {
        // Arrange & Act
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert
        var binanceClient = client as BinanceWebSocketClient;
        binanceClient!.Symbol.Should().Be("BTCUSDT");
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_CreatesClientWithCorrectExchangeName()
    {
        // Arrange & Act
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert
        var binanceClient = client as BinanceWebSocketClient;
        binanceClient!.ExchangeName.Should().Be("Binance");
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_CreatesClientWithCorrectName()
    {
        // Arrange & Act
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert
        var binanceClient = client as BinanceWebSocketClient;
        binanceClient!.Name.Should().Be("Binance_BTCUSDT");
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_CreatesClientWithDefaultState()
    {
        // Arrange & Act
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert
        var binanceClient = client as BinanceWebSocketClient;
        binanceClient.Should().NotBeNull();
        binanceClient!.IsConnected.Should().BeFalse();
        binanceClient.Symbol.Should().Be("BTCUSDT");
        binanceClient.ExchangeName.Should().Be("Binance");
    }

    [Fact(Timeout = 5000)]
    public void CreateBinanceClient_CreatesClientWithMonitoringIntegration()
    {
        // Arrange
        var client = _factory.CreateBinanceClient(
            "wss://stream.binance.com:9443/ws/{symbol}@trade",
            "BTCUSDT",
            "Binance");

        // Assert - Verify client properties
        client.Should().NotBeNull();
        client.ExchangeName.Should().Be("Binance");
        client.Symbol.Should().Be("BTCUSDT");
        client.Name.Should().Be("Binance_BTCUSDT");

        var binanceClient = client as BinanceWebSocketClient;
        binanceClient.Should().NotBeNull();

        // Act - Simulate events to verify monitoring integration
        binanceClient!.OnConnected();
        _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", MarketDataCollector.Core.Interfaces.ConnectionStatus.Connected, It.IsAny<string>()), Times.AtLeastOnce);

        binanceClient.OnDisconnected();
        _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", MarketDataCollector.Core.Interfaces.ConnectionStatus.Disconnected, It.IsAny<string>()), Times.AtLeastOnce);

        var testEx = new Exception("test error");
        binanceClient.OnErrorOccurred(testEx);
        _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", MarketDataCollector.Core.Interfaces.ConnectionStatus.Error, It.IsAny<string>()), Times.AtLeastOnce);

        binanceClient.OnMessageReceived("test message");
        _monitoringServiceMock.Verify(m => m.IncrementTickCounter("Binance"), Times.AtLeastOnce);
    }

    [Fact(Timeout = 5000)]
    public void CreateAllClients_CreatesAllClients()
    {
        // Arrange & Act
        var clients = _factory.CreateAllClients();

        // Assert
        clients.Should().NotBeNull();
        clients.Should().HaveCount(1);
    }

    [Fact(Timeout = 5000)]
    public void CreateAllClients_CreatesBinanceClients()
    {
        // Arrange & Act
        var clients = _factory.CreateAllClients();

        // Assert
        clients.Should().NotBeEmpty();
        clients.All(c => c is BinanceWebSocketClient).Should().BeTrue();
    }
}
