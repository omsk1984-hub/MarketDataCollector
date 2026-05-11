using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Application.Services;

public class MarketDataProcessorTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IRawTickRepository> _repositoryMock;
    private readonly Mock<ILogger<MarketDataProcessor>> _loggerMock;
    private readonly Mock<ITimeService> _timeServiceMock;

    public MarketDataProcessorTests(ITestOutputHelper output)
    {
        _output = output;
        _repositoryMock = new Mock<IRawTickRepository>();
        _loggerMock = new Mock<ILogger<MarketDataProcessor>>();
        _timeServiceMock = new Mock<ITimeService>();
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesProcessor()
    {
        // Arrange & Act
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 10,
            channelCapacity: 200);

        // Assert
        processor.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithZeroBatchSize_CreatesProcessor()
    {
        // Arrange & Act
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 0,
            channelCapacity: 100);

        // Assert
        processor.Should().NotBeNull();
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessTickAsync_WritesToChannel()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_WritesToChannel)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        var ticker = "BTCUSDT";
        var price = 1000.50m;
        var volume = 0.5m;
        var timestamp = DateTime.UtcNow;
        var exchange = "Binance";

        // Act
        await processor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);

        // Assert
        processor.Should().NotBeNull();
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessTickAsync_LogsDebugMessage()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_LogsDebugMessage)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        var ticker = "BTCUSDT";
        var price = 1000.50m;
        var volume = 0.5m;
        var timestamp = DateTime.UtcNow;
        var exchange = "Binance";

        // Act
        await processor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Тик добавлен в очередь")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessTickAsync_LogsTickerDetails()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_LogsTickerDetails)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        var ticker = "BTCUSDT";
        var price = 1000.50m;
        var volume = 0.5m;
        var timestamp = DateTime.UtcNow;
        var exchange = "Binance";

        // Act
        await processor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains(ticker) &&
                                                o.ToString()!.Contains(price.ToString()) &&
                                                o.ToString()!.Contains(volume.ToString()) &&
                                                o.ToString()!.Contains(exchange)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StartProcessingAsync_StartsBackgroundTask()
    {
        _output.WriteLine($"=== Running: {nameof(StartProcessingAsync_StartsBackgroundTask)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();

        // Act
        var task = processor.StartProcessingAsync(cts.Token);

        // Assert
        task.Should().NotBeNull();
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Обработчик рыночных данных запущен")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StartProcessingAsync_LogsBatchSize()
    {
        _output.WriteLine($"=== Running: {nameof(StartProcessingAsync_LogsBatchSize)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 10,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();

        // Act
        await processor.StartProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("10")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StopProcessingAsync_StopsProcessing()
    {
        _output.WriteLine($"=== Running: {nameof(StopProcessingAsync_StopsProcessing)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        
        await processor.StartProcessingAsync(cts.Token);
        await Task.Delay(50);

        // Act
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Обработчик рыночных данных остановлен")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StopProcessingAsync_LogsProcessedCount()
    {
        _output.WriteLine($"=== Running: {nameof(StopProcessingAsync_LogsProcessedCount)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        
        await processor.StartProcessingAsync(cts.Token);
        await Task.Delay(50);
        await processor.StopProcessingAsync(cts.Token);

        // Act
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Всего обработано")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }

    [Fact(Timeout = 10000)]
    public async Task GetProcessedCountAsync_ReturnsZeroInitially()
    {
        _output.WriteLine($"=== Running: {nameof(GetProcessedCountAsync_ReturnsZeroInitially)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        // Act
        var count = await processor.GetProcessedCountAsync();

        // Assert
        count.Should().Be(0);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessTickAsync_WithDifferentValues_WritesMultipleTicks()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_WithDifferentValues_WritesMultipleTicks)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance");

        // Assert
        processor.Should().NotBeNull();
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_SavesNewTicksToRepository()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_SavesNewTicksToRepository)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Wait for processing
        await Task.Delay(200);
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_SkipsDuplicateTicks()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_SkipsDuplicateTicks)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        // Первый тик не существует, второй - существует (дубликат)
        _repositoryMock.SetupSequence(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false)  // Первый тик не существует
            .ReturnsAsync(true);  // Второй тик существует

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance"); // Дубликат

        // Wait for processing
        await Task.Delay(200);
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsSkippedDuplicates()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsSkippedDuplicates)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Wait for processing
        await Task.Delay(200);
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("дубликатами")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        _repositoryMock.Setup(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        var errorOccurred = false;
        processor.OnError += (sender, ex) =>
        {
            errorOccurred = true;
            ex.Should().NotBeNull();
        };

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Wait for processing
        await Task.Delay(200);

        // Assert
        errorOccurred.Should().BeTrue();
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Критическая ошибка")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsSavedCount()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsSavedCount)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Wait for processing
        await Task.Delay(200);
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Батч сохранён")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsTotalProcessedEvery100()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsTotalProcessedEvery100)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _repositoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 1,
            channelCapacity: 100);

        _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act - добавляем 100 тиков
        for (int i = 0; i < 100; i++)
        {
            await processor.ProcessTickAsync("BTCUSDT", 1000.50m + i, 0.5m, DateTime.UtcNow, "Binance");
        }

        // Wait for processing
        await Task.Delay(500);
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Всего обработано тиков")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }
}
