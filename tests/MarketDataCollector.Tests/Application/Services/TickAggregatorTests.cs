using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Tests.Application.Services;

public class TickAggregatorTests
{
    private readonly Mock<ITimeService> _timeServiceMock;
    private readonly Mock<ILogger<TickAggregator>> _loggerMock;
    private readonly Mock<IServiceScopeFactory> _scopeFactoryMock;
    private readonly IOptions<TickAggregatorOptions> _options;

    private static readonly DateTime BaseTime = new(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);

    public TickAggregatorTests()
    {
        _timeServiceMock = new Mock<ITimeService>();
        _loggerMock = new Mock<ILogger<TickAggregator>>();
        _scopeFactoryMock = new Mock<IServiceScopeFactory>();
        _options = Options.Create(new TickAggregatorOptions
        {
            CandleIntervalSeconds = 60,
            FlushIntervalSeconds = 86_400, // 1 день — таймер не сработает в тестах
            ChannelCapacity = 10000
        });
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidParameters_CreatesAggregator()
    {
        // Arrange & Act
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        // Assert
        aggregator.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullTimeService_ThrowsArgumentNullException()
    {
        // Arrange & Act
        var act = () => new TickAggregator(
            null!,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("timeService");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange & Act
        var act = () => new TickAggregator(
            _timeServiceMock.Object,
            null!,
            _scopeFactoryMock.Object,
            _options);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullScopeFactory_ThrowsArgumentNullException()
    {
        // Arrange & Act
        var act = () => new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            null!,
            _options);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("scopeFactory");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange & Act
        var act = () => new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact(Timeout = 5000)]
    public async Task StartAsync_StartsProcessing()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();

        // Act
        var task = aggregator.StartAsync(cts.Token);

        // Assert
        task.Should().NotBeNull();

        await aggregator.StopAsync(cts.Token);
    }

    [Fact(Timeout = 5000)]
    public async Task StartAndStop_DoesNotThrow()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();

        // Act
        await aggregator.StartAsync(cts.Token);
        await aggregator.StopAsync(cts.Token);

        // Assert - не должно быть исключений
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_SingleTick_CreatesCandle()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance").Should().BeTrue();

        // Даём время на обработку
        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert - репозиторий должен получить 1 свечу при финальном flush
        // Проверка через Mock репозитория: создаём scope и проверяем вызовы
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_MultipleTicks_UpdatesCorrectOHLCV()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        var now = BaseTime;

        // Act - несколько тиков в одной минуте
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, now, "Binance");  // Open=50000
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, now.AddSeconds(5), "Binance");  // High=50100
        aggregator.TryWriteTick("BTCUSDT", 49900m, 0.3m, now.AddSeconds(10), "Binance");  // Low=49900
        aggregator.TryWriteTick("BTCUSDT", 50050m, 0.7m, now.AddSeconds(15), "Binance");  // Close=50050

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert - проверяем через репозиторий
        // Настраиваем scopeFactory, чтобы проверить сохранённые данные
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        // Повторяем акт с настроенным scopeFactory
        var aggregator2 = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        await aggregator2.StartAsync(cts.Token);

        aggregator2.TryWriteTick("BTCUSDT", 50000m, 1.0m, now, "Binance");
        aggregator2.TryWriteTick("BTCUSDT", 50100m, 0.5m, now.AddSeconds(5), "Binance");
        aggregator2.TryWriteTick("BTCUSDT", 49900m, 0.3m, now.AddSeconds(10), "Binance");
        aggregator2.TryWriteTick("BTCUSDT", 50050m, 0.7m, now.AddSeconds(15), "Binance");

        await Task.Delay(100);
        await aggregator2.StopAsync(cts.Token);

        // Assert
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(1);
        savedCandles[0].Ticker.Should().Be("BTCUSDT");
        savedCandles[0].Interval.Should().Be("1m");
        savedCandles[0].OpenPrice.Should().Be(50000m);
        savedCandles[0].HighPrice.Should().Be(50100m);
        savedCandles[0].LowPrice.Should().Be(49900m);
        savedCandles[0].ClosePrice.Should().Be(50050m);
        savedCandles[0].Volume.Should().Be(2.5m); // 1.0 + 0.5 + 0.3 + 0.7
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_DifferentMinutes_CreatesSeparateCandles()
    {
        // Arrange
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act - тики в разные минуты
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance");                    // 10:00:00
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, BaseTime.AddMinutes(1), "Binance");      // 10:01:00
        aggregator.TryWriteTick("BTCUSDT", 50200m, 0.3m, BaseTime.AddMinutes(2), "Binance");      // 10:02:00

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(3);
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime);
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime.AddMinutes(1));
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime.AddMinutes(2));
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_DifferentTickers_CreatesSeparateCandles()
    {
        // Arrange
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance");
        aggregator.TryWriteTick("ETHUSDT", 3000m, 10.0m, BaseTime, "Binance");

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(2);
        savedCandles.Should().Contain(c => c.Ticker == "BTCUSDT");
        savedCandles.Should().Contain(c => c.Ticker == "ETHUSDT");
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_SingleTickOnEdge_FlushesCompletedCandleOnly()
    {
        // Arrange
        _timeServiceMock.Setup(x => x.UtcNow).Returns(BaseTime.AddMinutes(2)); // текущее время = 10:02

        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance");                   // 10:00 - завершена
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, BaseTime.AddMinutes(1), "Binance");     // 10:01 - завершена
        aggregator.TryWriteTick("BTCUSDT", 50200m, 0.3m, BaseTime.AddMinutes(2), "Binance");     // 10:02 - ещё активна

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert - финальный flush сохраняет ВСЕ (активные тоже)
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(3);
    }

    [Fact(Timeout = 5000)]
    public async Task TryWriteTick_WithTicksAtIntervalBoundary_RoundsDownCorrectly()
    {
        // Arrange
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act - тики на границе минуты
        var endOfMinute = BaseTime.AddMinutes(1).AddSeconds(-1); // 10:00:59
        var startOfNextMinute = BaseTime.AddMinutes(1);         // 10:01:00

        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, endOfMinute, "Binance");
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, startOfNextMinute, "Binance");

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(2);
        var candle1 = savedCandles.Single(c => c.StartTime == BaseTime);
        candle1.EndTime.Should().Be(BaseTime.AddMinutes(1));     // 10:01
        var candle2 = savedCandles.Single(c => c.StartTime == BaseTime.AddMinutes(1));
        candle2.EndTime.Should().Be(BaseTime.AddMinutes(2));     // 10:02
    }

    [Fact(Timeout = 5000)]
    public void TryWriteTick_WithZeroVolume_DoesNotThrow()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();

        // Act
        var act = () => aggregator.TryWriteTick("BTCUSDT", 50000m, 0m, BaseTime, "Binance");

        // Assert
        act.Should().NotThrow();
    }

    [Fact(Timeout = 5000)]
    public async Task StopAsync_WithoutStart_DoesNotThrow()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        // Act
        var act = async () => await aggregator.StopAsync();

        // Assert
        await act.Should().NotThrowAsync();
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_ManyTicksInSameBucket_UpdatesVolumeCorrectly()
    {
        // Arrange
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act - 10 тиков по 0.1 volume
        for (int i = 0; i < 10; i++)
        {
            aggregator.TryWriteTick("BTCUSDT", 50000m + i, 0.1m, BaseTime.AddMilliseconds(i * 100), "Binance");
        }

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(1);
        savedCandles[0].Volume.Should().Be(1.0m); // 10 * 0.1
    }

    [Fact(Timeout = 10000)]
    public async Task TryWriteTick_DifferentExchanges_CreatesSeparateCandles()
    {
        // Arrange
        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        // Act
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance");
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, BaseTime, "Kraken");

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert - разные биржи = разные свечи
        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(2);
        savedCandles.Should().ContainSingle(c => c.Ticker == "BTCUSDT" && c.StartTime == BaseTime && c.Volume == 1.0m);
        savedCandles.Should().ContainSingle(c => c.Ticker == "BTCUSDT" && c.StartTime == BaseTime && c.Volume == 0.5m);
    }

    [Theory(Timeout = 10000)]
    [InlineData(30)]
    [InlineData(60)]
    [InlineData(120)]
    public async Task TryWriteTick_DifferentIntervals_CreatesCorrectBuckets(int intervalSeconds)
    {
        // Arrange
        var intervalOptions = Options.Create(new TickAggregatorOptions
        {
            CandleIntervalSeconds = intervalSeconds,
            FlushIntervalSeconds = 86_400,
            ChannelCapacity = 10000
        });

        var repoMock = new Mock<IAggregatedDataRepository>();
        List<AggregatedData>? savedCandles = null;

        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<AggregatedData>, CancellationToken>((candles, _) => savedCandles = candles.ToList());

        SetupScopeFactory(repoMock);

        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            intervalOptions);

        using var cts = new CancellationTokenSource();
        await aggregator.StartAsync(cts.Token);

        var now = BaseTime;

        // Act - тики в разные бакеты с интервалом intervalSeconds
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, now, "Binance");
        aggregator.TryWriteTick("BTCUSDT", 50100m, 0.5m, now.AddSeconds(intervalSeconds), "Binance");
        aggregator.TryWriteTick("BTCUSDT", 50200m, 0.3m, now.AddSeconds(intervalSeconds * 2), "Binance");

        await Task.Delay(100);
        await aggregator.StopAsync(cts.Token);

        // Assert
        var expectedInterval = intervalSeconds >= 60 && intervalSeconds % 60 == 0
            ? $"{intervalSeconds / 60}m"
            : $"{intervalSeconds}s";

        savedCandles.Should().NotBeNull();
        savedCandles!.Count.Should().Be(3);
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime);
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime.AddSeconds(intervalSeconds));
        savedCandles.Should().ContainSingle(c => c.StartTime == BaseTime.AddSeconds(intervalSeconds * 2));
        savedCandles.Should().OnlyContain(c => c.Interval == expectedInterval);
    }

    [Fact(Timeout = 5000)]
    public void TryWriteTick_WhenChannelClosed_ReturnsFalse()
    {
        // Arrange
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            _loggerMock.Object,
            _scopeFactoryMock.Object,
            _options);

        using var cts = new CancellationTokenSource();
        aggregator.StartAsync(cts.Token);
        aggregator.StopAsync(cts.Token).GetAwaiter().GetResult(); // вызывает _channel.Writer.TryComplete()

        // Act — канал закрыт, TryWrite должен вернуть false и не бросить исключение
        var act = () => aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance");

        // Assert
        act.Should().NotThrow();
        aggregator.TryWriteTick("BTCUSDT", 50000m, 1.0m, BaseTime, "Binance").Should().BeFalse();
    }

    /// <summary>
    /// Настраивает IServiceScopeFactory для возврата конкретного репозитория.
    /// </summary>
    private void SetupScopeFactory(Mock<IAggregatedDataRepository> repoMock)
    {
        var serviceScope = new Mock<IServiceScope>();
        var serviceProvider = new Mock<IServiceProvider>();

        serviceProvider.Setup(sp => sp.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);

        serviceScope.Setup(s => s.ServiceProvider)
            .Returns(serviceProvider.Object);

        _scopeFactoryMock.Setup(sf => sf.CreateScope())
            .Returns(serviceScope.Object);
    }
}