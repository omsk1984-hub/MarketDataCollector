using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Data;
using MarketDataCollector.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MarketDataCollector.Tests.Infrastructure.Repositories;

public class RawTickRepositoryTests
{
    private readonly MarketDataDbContext _context;
    private readonly RawTickRepository _repository;

    public RawTickRepositoryTests()
    {
        var options = new DbContextOptionsBuilder<MarketDataDbContext>()
            .UseInMemoryDatabase(databaseName: Guid.NewGuid().ToString())
            .Options;

        _context = new MarketDataDbContext(options);
        _repository = new RawTickRepository(_context);
    }

    [Fact]
    public async Task GetByIdAsync_WhenExists_ReturnsTick()
    {
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

    [Fact]
    public async Task GetByIdAsync_WhenNotExists_ReturnsNull()
    {
        // Arrange
        var nonExistentId = Guid.NewGuid();

        // Act
        var result = await _repository.GetByIdAsync(nonExistentId);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task GetAllAsync_ReturnsAllTicks()
    {
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

    [Fact]
    public async Task AddAsync_AddsTickToDatabase()
    {
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

    [Fact]
    public async Task AddRangeAsync_AddsMultipleTicksToDatabase()
    {
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

    [Fact]
    public async Task Update_UpdatesTickInDatabase()
    {
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

    [Fact]
    public async Task Remove_RemovesTickFromDatabase()
    {
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

    [Fact]
    public async Task GetByTickerAsync_ReturnsTicksForTicker()
    {
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

    [Fact]
    public async Task GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate()
    {
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

    [Fact]
    public async Task GetByTickerAsync_WithToDate_ReturnsTicksUntilThatDate()
    {
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

    [Fact]
    public async Task GetByTickerAsync_WithDateRange_ReturnsTicksInRange()
    {
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

    [Fact]
    public async Task GetByTickerAsync_ReturnsTicksOrderedByTimestamp()
    {
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

    [Fact]
    public async Task GetByExchangeAsync_ReturnsTicksForExchange()
    {
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

    [Fact]
    public async Task GetByExchangeAsync_WithDateRange_ReturnsTicksInRange()
    {
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

    [Fact]
    public async Task ExistsAsync_WhenTickExists_ReturnsTrue()
    {
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", tick.Timestamp);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WhenTickDoesNotExist_ReturnsFalse()
    {
        // Arrange
        var differentTimestamp = DateTime.UtcNow.AddSeconds(1);

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", differentTimestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithDifferentTicker_ReturnsFalse()
    {
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("ETHUSDT", "Binance", tick.Timestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithDifferentExchange_ReturnsFalse()
    {
        // Arrange
        var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
        await _repository.AddAsync(tick);
        await _repository.SaveChangesAsync();

        // Act
        var exists = await _repository.ExistsAsync("BTCUSDT", "Kraken", tick.Timestamp);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task GetCountAsync_ReturnsTotalCount()
    {
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

    [Fact]
    public async Task GetCountAsync_WithDateRange_ReturnsCountInRange()
    {
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

    [Fact]
    public async Task FindAsync_WithPredicate_ReturnsMatchingTicks()
    {
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
