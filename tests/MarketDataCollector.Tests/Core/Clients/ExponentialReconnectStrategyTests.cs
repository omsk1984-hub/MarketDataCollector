using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;

namespace MarketDataCollector.Tests.Core.Clients;

public class ExponentialReconnectStrategyTests
{
    private readonly WebSocketClientOptions _defaultOptions;
    private readonly Mock<ILogger<ExponentialReconnectStrategy>> _loggerMock;

    public ExponentialReconnectStrategyTests()
    {
        _defaultOptions = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60)
        };
        _loggerMock = new Mock<ILogger<ExponentialReconnectStrategy>>();
    }

    [Fact]
    public void Constructor_WithValidOptions_SetsProperties()
    {
        // Arrange & Act
        var strategy = new ExponentialReconnectStrategy(
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        // Assert
        strategy.Should().NotBeNull();
    }

    [Fact]
    public void GetDelay_FirstAttempt_ReturnsBaseDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(1);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void GetDelay_SecondAttempt_ReturnsDoubleDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(2);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public void GetDelay_ThirdAttempt_ReturnsExponentialDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(3);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(8));
    }

    [Fact]
    public void GetDelay_FourthAttempt_ReturnsExponentialDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(4);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(16));
    }

    [Fact]
    public void GetDelay_ExceedsMaxDelay_ReturnsCappedDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(10)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(5); // 2 * 2^4 = 32, but capped at 10

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void GetDelay_WithLargeAttempt_ReturnsMaxDelay()
    {
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(30)
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act
        var delay = strategy.GetDelay(20);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void GetDelay_WithZeroAttempt_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        // Act & Assert
        var act = () => strategy.GetDelay(0);
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("attempt");
    }

    [Fact]
    public void GetDelay_WithNegativeAttempt_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        // Act & Assert
        var act = () => strategy.GetDelay(-1);
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("attempt");
    }

    [Fact]
    public void ShouldRetry_AlwaysReturnsTrue()
    {
        // Arrange
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        // Act
        var result1 = strategy.ShouldRetry(1);
        var result2 = strategy.ShouldRetry(10);
        var result3 = strategy.ShouldRetry(100);

        // Assert
        result1.Should().BeTrue();
        result2.Should().BeTrue();
        result3.Should().BeTrue();
    }

    [Fact]
    public void Reset_LogsDebugMessage()
    {
        // Arrange
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        // Act
        strategy.Reset();

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Сброс состояния стратегии переподключения")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }
}
