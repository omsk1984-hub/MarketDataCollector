using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Services;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Application.Services;

public class DataStorageServiceTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IRawTickRepository> _repositoryMock;
    private readonly Mock<ILogger<DataStorageService>> _loggerMock;

    public DataStorageServiceTests(ITestOutputHelper output)
    {
        _output = output;
        _repositoryMock = new Mock<IRawTickRepository>();
        _loggerMock = new Mock<ILogger<DataStorageService>>();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidParameters_CreatesService()
    {
        // Arrange & Act
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullRepository_ThrowsArgumentNullException()
    {
        // Arrange
        IRawTickRepository? nullRepository = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new DataStorageService(
            nullRepository,
            _loggerMock.Object));
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTickAsync_SavesTickToRepository()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTickAsync_SavesTickToRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", 
            new SystemTimeService());

        // Act
        await service.StoreRawTickAsync(tick);

        // Assert
        _repositoryMock.Verify(x => x.AddAsync(tick, It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTickAsync_LogsDebugMessage()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTickAsync_LogsDebugMessage)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance",
            new SystemTimeService());

        // Act
        await service.StoreRawTickAsync(tick);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Raw tick stored")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTickAsync_LogsTickDetails()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTickAsync_LogsTickDetails)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance",
            new SystemTimeService());

        // Act
        await service.StoreRawTickAsync(tick);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains(tick.Id.ToString()) &&
                                                o.ToString()!.Contains(tick.Ticker) &&
                                                o.ToString()!.Contains(tick.Price.ToString())),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance",
            new SystemTimeService());

        _repositoryMock.Setup(x => x.AddAsync(tick, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.StoreRawTickAsync(tick));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error storing raw tick")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
        // Проверяем, что SaveChangesAsync НЕ вызывался после ошибки
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTicksBatchAsync_SavesBatchToRepository()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTicksBatchAsync_SavesBatchToRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        // Act
        await service.StoreRawTicksBatchAsync(ticks);

        // Assert
        _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTicksBatchAsync_LogsBatchSize()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTicksBatchAsync_LogsBatchSize)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        // Act
        await service.StoreRawTicksBatchAsync(ticks);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Batch of") &&
                                                o.ToString()!.Contains("2")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTicksBatchAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTicksBatchAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        _repositoryMock.Setup(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.StoreRawTicksBatchAsync(ticks));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error storing batch")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task StoreRawTicksBatchAsync_WithEmptyCollection_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(StoreRawTicksBatchAsync_WithEmptyCollection_DoesNothing)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        // Act
        await service.StoreRawTicksBatchAsync(new List<RawTick>());

        // Assert
        _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTicksByTickerAsync_ReturnsTicksFromRepository()
    {
        _output.WriteLine($"=== Running: {nameof(GetTicksByTickerAsync_ReturnsTicksFromRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var expectedTicks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        _repositoryMock.Setup(x => x.GetByTickerAsync("BTCUSDT", null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedTicks);

        // Act
        var result = await service.GetTicksByTickerAsync("BTCUSDT");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(2);
        result.First().Ticker.Should().Be("BTCUSDT");
    }

    [Fact(Timeout = 10000)]
    public async Task GetTicksByTickerAsync_WithDateRange_ReturnsTicksFromRepository()
    {
        _output.WriteLine($"=== Running: {nameof(GetTicksByTickerAsync_WithDateRange_ReturnsTicksFromRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var expectedTicks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        var from = DateTime.UtcNow.AddDays(-1);
        var to = DateTime.UtcNow;

        _repositoryMock.Setup(x => x.GetByTickerAsync("BTCUSDT", from, to, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedTicks);

        // Act
        var result = await service.GetTicksByTickerAsync("BTCUSDT", from, to);

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(1);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTicksByTickerAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(GetTicksByTickerAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.GetByTickerAsync("BTCUSDT", null, null, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.GetTicksByTickerAsync("BTCUSDT"));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error retrieving ticks for ticker")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTicksByExchangeAsync_ReturnsTicksFromRepository()
    {
        _output.WriteLine($"=== Running: {nameof(GetTicksByExchangeAsync_ReturnsTicksFromRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var expectedTicks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        _repositoryMock.Setup(x => x.GetByExchangeAsync("Binance", null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedTicks);

        // Act
        var result = await service.GetTicksByExchangeAsync("Binance");

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(2);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTicksByExchangeAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(GetTicksByExchangeAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.GetByExchangeAsync("Binance", null, null, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.GetTicksByExchangeAsync("Binance"));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error retrieving ticks for exchange")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTotalTicksCountAsync_ReturnsCountFromRepository()
    {
        _output.WriteLine($"=== Running: {nameof(GetTotalTicksCountAsync_ReturnsCountFromRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.GetCountAsync(null, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(100);

        // Act
        var count = await service.GetTotalTicksCountAsync();

        // Assert
        count.Should().Be(100);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTotalTicksCountAsync_WithDateRange_ReturnsCountFromRepository()
    {
        _output.WriteLine($"=== Running: {nameof(GetTotalTicksCountAsync_WithDateRange_ReturnsCountFromRepository)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var from = DateTime.UtcNow.AddDays(-1);
        var to = DateTime.UtcNow;

        _repositoryMock.Setup(x => x.GetCountAsync(from, to, It.IsAny<CancellationToken>()))
            .ReturnsAsync(50);

        // Act
        var count = await service.GetTotalTicksCountAsync(from, to);

        // Assert
        count.Should().Be(50);
    }

    [Fact(Timeout = 10000)]
    public async Task GetTotalTicksCountAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(GetTotalTicksCountAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.GetCountAsync(null, null, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.GetTotalTicksCountAsync());

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error getting total ticks count")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task TickExistsAsync_ReturnsTrueWhenTickExists()
    {
        _output.WriteLine($"=== Running: {nameof(TickExistsAsync_ReturnsTrueWhenTickExists)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.ExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        var exists = await service.TickExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact(Timeout = 10000)]
    public async Task TickExistsAsync_ReturnsFalseWhenTickDoesNotExist()
    {
        _output.WriteLine($"=== Running: {nameof(TickExistsAsync_ReturnsFalseWhenTickDoesNotExist)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.ExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        // Act
        var exists = await service.TickExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact(Timeout = 10000)]
    public async Task TickExistsAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(TickExistsAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.ExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.TickExistsAsync("BTCUSDT", "Binance", DateTime.UtcNow));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error checking if tick exists")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task GetUnnormalizedTicksAsync_CallsRepositoryWithCorrectLimit()
    {
        _output.WriteLine($"=== Running: {nameof(GetUnnormalizedTicksAsync_CallsRepositoryWithCorrectLimit)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var expectedTicks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow.AddMinutes(-1), "Binance", new SystemTimeService())
        };

        _repositoryMock.Setup(x => x.GetUnnormalizedAsync(1000, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedTicks);

        // Act
        var result = await service.GetUnnormalizedTicksAsync(limit: 1000);

        // Assert
        result.Should().HaveCount(2);
        _repositoryMock.Verify(x => x.GetUnnormalizedAsync(1000, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task GetUnnormalizedTicksAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(GetUnnormalizedTicksAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        _repositoryMock.Setup(x => x.GetUnnormalizedAsync(1000, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.GetUnnormalizedTicksAsync());

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error retrieving unnormalized ticks")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task MarkAsNormalizedAsync_WhenTickExists_MarksAsNormalized()
    {
        _output.WriteLine($"=== Running: {nameof(MarkAsNormalizedAsync_WhenTickExists_MarksAsNormalized)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tickId = Guid.NewGuid();
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance",
            new SystemTimeService());

        _repositoryMock.Setup(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(tick);

        // Act
        await service.MarkAsNormalizedAsync(tickId);

        // Assert
        _repositoryMock.Verify(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.Update(tick), Times.Once);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 10000)]
    public async Task MarkAsNormalizedAsync_WhenTickNotFound_DoesNothing()
    {
        _output.WriteLine($"=== Running: {nameof(MarkAsNormalizedAsync_WhenTickNotFound_DoesNothing)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tickId = Guid.NewGuid();

        _repositoryMock.Setup(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((RawTick?)null);

        // Act
        await service.MarkAsNormalizedAsync(tickId);

        // Assert
        _repositoryMock.Verify(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()), Times.Once);
        _repositoryMock.Verify(x => x.Update(It.IsAny<RawTick>()), Times.Never);
        _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact(Timeout = 10000)]
    public async Task MarkAsNormalizedAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
    {
        _output.WriteLine($"=== Running: {nameof(MarkAsNormalizedAsync_WhenRepositoryThrows_LogsErrorAndRethrows)} ===");
        // Arrange
        var service = new DataStorageService(
            _repositoryMock.Object,
            _loggerMock.Object);

        var tickId = Guid.NewGuid();

        _repositoryMock.Setup(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.MarkAsNormalizedAsync(tickId));

        exception.Message.Should().Be("Database error");
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error marking tick as normalized")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }
}
