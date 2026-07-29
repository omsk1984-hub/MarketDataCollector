using MarketDataCollector.Application.Services;

namespace MarketDataCollector.Tests.Application.Services;

public class DeduplicationCacheTests
{
    [Fact]
    public void Contains_ReturnsFalse_WhenEmpty()
    {
        // Arrange
        var cache = new DeduplicationCache(100);

        // Act & Assert
        cache.Contains("BTCUSDT", "binance", DateTime.UtcNow).Should().BeFalse();
    }

    [Fact]
    public void Contains_ReturnsTrue_AfterAdd()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", timestamp);

        // Assert
        cache.Contains("BTCUSDT", "binance", timestamp).Should().BeTrue();
    }

    [Fact]
    public void Contains_ReturnsFalse_ForDifferentTicker()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", timestamp);

        // Assert
        cache.Contains("ETHUSDT", "binance", timestamp).Should().BeFalse();
    }

    [Fact]
    public void Contains_ReturnsFalse_ForDifferentExchange()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", timestamp);

        // Assert
        cache.Contains("BTCUSDT", "kraken", timestamp).Should().BeFalse();
    }

    [Fact]
    public void Contains_ReturnsFalse_ForDifferentTimestamp()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var ts1 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ts2 = new DateTime(2024, 1, 1, 0, 0, 1, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", ts1);

        // Assert
        cache.Contains("BTCUSDT", "binance", ts2).Should().BeFalse();
    }

    [Fact]
    public void Add_EvictsOldest_WhenMaxSizeReached()
    {
        // Arrange
        var cache = new DeduplicationCache(3);
        var ts1 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ts2 = new DateTime(2024, 1, 1, 0, 0, 1, DateTimeKind.Utc);
        var ts3 = new DateTime(2024, 1, 1, 0, 0, 2, DateTimeKind.Utc);
        var ts4 = new DateTime(2024, 1, 1, 0, 0, 3, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", ts1);
        cache.Add("BTCUSDT", "binance", ts2);
        cache.Add("BTCUSDT", "binance", ts3);
        cache.Add("BTCUSDT", "binance", ts4); // должна вытеснить ts1

        // Assert
        cache.Contains("BTCUSDT", "binance", ts1).Should().BeFalse(); // evicted
        cache.Contains("BTCUSDT", "binance", ts2).Should().BeTrue();
        cache.Contains("BTCUSDT", "binance", ts3).Should().BeTrue();
        cache.Contains("BTCUSDT", "binance", ts4).Should().BeTrue();
        cache.Count.Should().Be(3);
    }

    [Fact]
    public void Add_DoesNotGrowBeyondMaxSize()
    {
        // Arrange
        var cache = new DeduplicationCache(5);

        // Act — добавляем 10 записей
        for (int i = 0; i < 10; i++)
        {
            var ts = new DateTime(2024, 1, 1, 0, 0, i, DateTimeKind.Utc);
            cache.Add("BTCUSDT", "binance", ts);
        }

        // Assert
        cache.Count.Should().Be(5);
    }

    [Fact]
    public void Add_IgnoresDuplicateKey()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", timestamp);
        cache.Add("BTCUSDT", "binance", timestamp); // дубликат

        // Assert
        cache.Count.Should().Be(1);
    }

    [Fact]
    public void Count_ReflectsActualSize()
    {
        // Arrange
        var cache = new DeduplicationCache(100);

        // Act & Assert
        cache.Count.Should().Be(0);

        cache.Add("BTCUSDT", "binance", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        cache.Count.Should().Be(1);

        cache.Add("ETHUSDT", "binance", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        cache.Count.Should().Be(2);
    }

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        // Arrange
        var cache = new DeduplicationCache(100);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        cache.Add("BTCUSDT", "binance", timestamp);

        // Act
        cache.Clear();

        // Assert
        cache.Count.Should().Be(0);
        cache.Contains("BTCUSDT", "binance", timestamp).Should().BeFalse();
    }

    [Fact]
    public void Contains_ReturnsFalse_WhenMaxSizeIsZero()
    {
        // Arrange — кэш отключён
        var cache = new DeduplicationCache(0);
        var timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Act
        cache.Add("BTCUSDT", "binance", timestamp);

        // Assert — ничего не хранится
        cache.Contains("BTCUSDT", "binance", timestamp).Should().BeFalse();
        cache.Count.Should().Be(0);
    }
}
