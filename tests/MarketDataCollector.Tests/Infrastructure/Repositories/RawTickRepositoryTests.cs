using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Data;
using MarketDataCollector.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Infrastructure.Repositories;

public class RawTickRepositoryTests
{
    private readonly ITestOutputHelper _output;
    private readonly MarketDataDbContext _context;
    private readonly RawTickRepository _repository;

    public RawTickRepositoryTests(ITestOutputHelper output)
    {
        _output = output;
        var options = new DbContextOptionsBuilder<MarketDataDbContext>()
            .UseInMemoryDatabase(databaseName: Guid.NewGuid().ToString())
            .Options;

        _context = new MarketDataDbContext(options);
        _repository = new RawTickRepository(_context);
    }

    [Fact(Timeout = 10000)]
    public async Task GetByIdAsync_WhenExists_ReturnsTick()
    {
        _output.WriteLine($"=== Running: {nameof(GetByIdAsync_WhenExists_ReturnsTick)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByIdAsync(tick.Id);

        // Assert
        result.Should().NotBeNull();
        result!.Id.Should().Be(tick.Id);
        result.Ticker.Should().Be("BTCUSDT");
    }

    [Fact(Timeout = 10000)]
    public async Task GetByIdAsync_WhenNotExists_ReturnsNull()
    {
        _output.WriteLine($"=== Running: {nameof(GetByIdAsync_WhenNotExists_ReturnsNull)} ===");
        // Arrange
        var nonExistentId = Guid.NewGuid();

        // Act
        var result = await _repository.GetByIdAsync(nonExistentId);

        // Assert
        result.Should().BeNull();
    }

    [Fact(Timeout = 10000)]
    public async Task GetAllAsync_ReturnsAllTicks()
    {
        _output.WriteLine($"=== Running: {nameof(GetAllAsync_ReturnsAllTicks)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetAllAsync();

        // Assert
        result.Should().HaveCount(3);
    }

    [Fact(Timeout = 10000)]
    public async Task AddAsync_AddsTickToDatabase()
    {
        _output.WriteLine($"=== Running: {nameof(AddAsync_AddsTickToDatabase)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());

        // Act
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Assert
        var result = await _repository.GetByIdAsync(tick.Id);
        result.Should().NotBeNull();
        result!.Ticker.Should().Be("BTCUSDT");
    }

    [Fact(Timeout = 10000)]
    public async Task AddRangeAsync_AddsMultipleTicksToDatabase()
    {
        _output.WriteLine($"=== Running: {nameof(AddRangeAsync_AddsMultipleTicksToDatabase)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        // Act
        await _repository.AddRangeAsync(ticks);
        await _repository.SaveChangesAsync();

        // Assert
        var allTicks = await _repository.GetAllAsync();
        allTicks.Should().HaveCount(3);
    }

    [Fact(Timeout = 10000)]
    public async Task Update_UpdatesTickInDatabase()
    {
        _output.WriteLine($"=== Running: {nameof(Update_UpdatesTickInDatabase)} ===");
        // Arrange
        var timeService = new SystemTimeService();
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", timeService);
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act - Обновляем цену через метод Update
        tick.UpdatePrice(2000.00m);
        _repository.Update(tick);
        await _repository.SaveChangesAsync();

        // Assert
        var result = await _repository.GetByIdAsync(tick.Id);
        result!.Price.Should().Be(2000.00m);
    }

    [Fact(Timeout = 10000)]
    public async Task Remove_RemovesTickFromDatabase()
    {
        _output.WriteLine($"=== Running: {nameof(Remove_RemovesTickFromDatabase)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        _repository.Remove(tick);
        await _repository.SaveChangesAsync();

        // Assert
        var result = await _repository.GetByIdAsync(tick.Id);
        result.Should().BeNull();
    }

    [Fact(Timeout = 10000)]
    public async Task GetByTickerAsync_ReturnsTicksForTicker()
    {
        _output.WriteLine($"=== Running: {nameof(GetByTickerAsync_ReturnsTicksForTicker)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByTickerAsync("BTCUSDT");

        // Assert
        result.Should().HaveCount(2);
        result.All(t => t.Ticker == "BTCUSDT").Should().BeTrue();
    }

    [Fact(Timeout = 10000)]
    public async Task GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate()
    {
        _output.WriteLine($"=== Running: {nameof(GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate)} ===");
        // Arrange
        var now = DateTime.UtcNow;
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByTickerAsync("BTCUSDT", now.AddDays(-1.5), null);

        // Assert
        result.Should().HaveCount(2);
        result.All(t => t.Timestamp >= now.AddDays(-1.5));
    }

    [Fact(Timeout = 10000)]
    public async Task GetByTickerAsync_WithToDate_ReturnsTicksUntilThatDate()
    {
        _output.WriteLine($"=== Running: {nameof(GetByTickerAsync_WithToDate_ReturnsTicksUntilThatDate)} ===");
        // Arrange
        var now = DateTime.UtcNow;
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByTickerAsync("BTCUSDT", null, now.AddDays(-1.5));

        // Assert
        result.Should().HaveCount(1);
        result.First().Timestamp.Should().Be(now.AddDays(-2));
    }

    [Fact(Timeout = 10000)]
    public async Task GetByTickerAsync_WithDateRange_ReturnsTicksInRange()
    {
        _output.WriteLine($"=== Running: {nameof(GetByTickerAsync_WithDateRange_ReturnsTicksInRange)} ===");
        // Arrange
        var now = DateTime.UtcNow;
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByTickerAsync("BTCUSDT", now.AddDays(-1.5), now.AddDays(-0.5));

        // Assert
        result.Should().HaveCount(1);
        result.First().Timestamp.Should().Be(now.AddDays(-1));
    }

    [Fact(Timeout = 10000)]
    public async Task GetByTickerAsync_ReturnsTicksOrderedByTimestamp()
    {
        _output.WriteLine($"=== Running: {nameof(GetByTickerAsync_ReturnsTicksOrderedByTimestamp)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1002.00m, 0.2m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow.AddDays(-1), "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByTickerAsync("BTCUSDT");

        // Assert
        result.First().Timestamp.Should().Be(DateTime.UtcNow.AddDays(-2));
        result.Last().Timestamp.Should().Be(DateTime.UtcNow);
    }

    [Fact(Timeout = 10000)]
    public async Task GetByExchangeAsync_ReturnsTicksForExchange()
    {
        _output.WriteLine($"=== Running: {nameof(GetByExchangeAsync_ReturnsTicksForExchange)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Kraken", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByExchangeAsync("Binance");

        // Assert
        result.Should().HaveCount(2);
        result.All(t => t.Exchange == "Binance").Should().BeTrue();
    }

    [Fact(Timeout = 10000)]
    public async Task GetByExchangeAsync_WithDateRange_ReturnsTicksInRange()
    {
        _output.WriteLine($"=== Running: {nameof(GetByExchangeAsync_WithDateRange_ReturnsTicksInRange)} ===");
        // Arrange
        var now = DateTime.UtcNow;
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.GetByExchangeAsync("Binance", now.AddDays(-1.5), now.AddDays(-0.5));

        // Assert
        result.Should().HaveCount(1);
        result.First().Timestamp.Should().Be(now.AddDays(-1));
    }

    [Fact(Timeout = 10000)]
    public async Task ExistsAsync_WhenTickExists_ReturnsTrue()
    {
        _output.WriteLine($"=== Running: {nameof(ExistsAsync_WhenTickExists_ReturnsTrue)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", tick.Timestamp);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact(Timeout = 10000)]
    public async Task ExistsAsync_WhenTickDoesNotExist_ReturnsFalse()
    {
        _output.WriteLine($"=== Running: {nameof(ExistsAsync_WhenTickDoesNotExist_ReturnsFalse)} ===");
        // Arrange
        var differentTimestamp = DateTime.UtcNow.AddSeconds(1);

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", differentTimestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact(Timeout = 10000)]
    public async Task ExistsAsync_WithDifferentTicker_ReturnsFalse()
    {
        _output.WriteLine($"=== Running: {nameof(ExistsAsync_WithDifferentTicker_ReturnsFalse)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("ETHUSDT", "Binance", tick.Timestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact(Timeout = 10000)]
    public async Task ExistsAsync_WithDifferentExchange_ReturnsFalse()
    {
        _output.WriteLine($"=== Running: {nameof(ExistsAsync_WithDifferentExchange_ReturnsFalse)} ===");
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Kraken", tick.Timestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact(Timeout = 10000)]
    public async Task GetCountAsync_ReturnsTotalCount()
    {
        _output.WriteLine($"=== Running: {nameof(GetCountAsync_ReturnsTotalCount)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var count = await _repository.GetCountAsync();

        // Assert
        count.Should().Be(3);
    }

    [Fact(Timeout = 10000)]
    public async Task GetCountAsync_WithDateRange_ReturnsCountInRange()
    {
        _output.WriteLine($"=== Running: {nameof(GetCountAsync_WithDateRange_ReturnsCountInRange)} ===");
        // Arrange
        var now = DateTime.UtcNow;
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var count = await _repository.GetCountAsync(now.AddDays(-1.5), now.AddDays(-0.5));

        // Assert
        count.Should().Be(1);
    }

    [Fact(Timeout = 10000)]
    public async Task FindAsync_WithPredicate_ReturnsMatchingTicks()
    {
        _output.WriteLine($"=== Running: {nameof(FindAsync_WithPredicate_ReturnsMatchingTicks)} ===");
        // Arrange
        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        foreach (var tick in ticks)
        {
            await _repository.AddAsync(tick);
        }
        await _repository.SaveChangesAsync();

        // Act
        var result = await _repository.FindAsync(t => t.Ticker == "BTCUSDT");

        // Assert
        result.Should().HaveCount(2);
        result.Should().AllBeOfType<RawTick>();
        result.All(t => t.Ticker == "BTCUSDT").Should().BeTrue();
    }
}
