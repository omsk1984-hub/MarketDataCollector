using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Core.Clients;

public class ExponentialReconnectStrategyTests
{
    private readonly ITestOutputHelper _output;
    private readonly WebSocketClientOptions _defaultOptions;
    private readonly Mock<ILogger<ExponentialReconnectStrategy>> _loggerMock;

    public ExponentialReconnectStrategyTests(ITestOutputHelper output)
    {
        _output = output;
        _defaultOptions = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0 // точные проверки без jitter
        };
        _loggerMock = new Mock<ILogger<ExponentialReconnectStrategy>>();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidOptions_SetsProperties()
    {
        var strategy = new ExponentialReconnectStrategy(
            Options.Create(_defaultOptions),
            _loggerMock.Object);

        strategy.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_FirstAttempt_ReturnsBaseDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(1);

        delay.Should().Be(TimeSpan.FromSeconds(2));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_SecondAttempt_ReturnsDoubleDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(2);

        delay.Should().Be(TimeSpan.FromSeconds(4));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_ThirdAttempt_ReturnsExponentialDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(3);

        delay.Should().Be(TimeSpan.FromSeconds(8));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_FourthAttempt_ReturnsExponentialDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(4);

        delay.Should().Be(TimeSpan.FromSeconds(16));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_ExceedsMaxDelay_ReturnsCappedDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(2),
            MaxReconnectDelay = TimeSpan.FromSeconds(10),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(5); // 2 * 2^4 = 32, but capped at 10

        delay.Should().Be(TimeSpan.FromSeconds(10));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_WithLargeAttempt_ReturnsMaxDelay()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(30),
            JitterFactor = 0
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        var delay = strategy.GetDelay(20);

        delay.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_WithZeroAttempt_ThrowsArgumentOutOfRangeException()
    {
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        var act = () => strategy.GetDelay(0);
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("attempt");
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_WithNegativeAttempt_ThrowsArgumentOutOfRangeException()
    {
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        var act = () => strategy.GetDelay(-1);
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("attempt");
    }

    [Fact(Timeout = 5000)]
    public void GetDelay_WithJitter_ReturnsDelayWithinRange()
    {
        // Arrange — JitterFactor = 0.3 (по умолчанию)
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(10),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            JitterFactor = 0.3
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        // Act — запускаем 100 раз и проверяем разброс
        var delays = Enumerable.Range(0, 100)
            .Select(_ => strategy.GetDelay(1).TotalSeconds)
            .ToList();

        // Assert — все задержки в диапазоне [7, 13] (10 ± 30%)
        delays.Should().AllSatisfy(d => d.Should().BeInRange(7.0, 13.0));
        // Разброс должен быть значительным — не все одинаковые
        delays.Distinct().Count().Should().BeGreaterThan(1);
    }

    [Fact(Timeout = 5000)]
    public void ShouldRetry_WithZeroMaxAttempts_ReturnsTrueAlways()
    {
        // MaxReconnectAttempts = 0 → бесконечно
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        strategy.ShouldRetry(1).Should().BeTrue();
        strategy.ShouldRetry(10).Should().BeTrue();
        strategy.ShouldRetry(100).Should().BeTrue();
    }

    [Fact(Timeout = 5000)]
    public void ShouldRetry_WithMaxAttempts_RespectsLimit()
    {
        var options = new WebSocketClientOptions
        {
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxReconnectAttempts = 5
        };
        var strategy = new ExponentialReconnectStrategy(Options.Create(options), _loggerMock.Object);

        strategy.ShouldRetry(1).Should().BeTrue();
        strategy.ShouldRetry(5).Should().BeTrue();
        strategy.ShouldRetry(6).Should().BeFalse();
        strategy.ShouldRetry(100).Should().BeFalse();
    }

    [Fact(Timeout = 5000)]
    public void Reset_LogsDebugMessage()
    {
        var strategy = new ExponentialReconnectStrategy(Options.Create(_defaultOptions), _loggerMock.Object);

        strategy.Reset();

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
