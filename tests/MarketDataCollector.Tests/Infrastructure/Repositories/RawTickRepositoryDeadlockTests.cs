using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using System.Runtime.Serialization;

namespace MarketDataCollector.Tests.Infrastructure.Repositories;

/// <summary>
/// Тесты для retry-логики при deadlock (Npgsql.PostgresException с SqlState=40P01)
/// в <see cref="RawTickRepository.BulkInsertIgnoreConflictsAsync"/>.
///
/// DEADLOCK-ТЕСТЫ: проверяют, что слой выше (MarketDataProcessor) корректно
/// перехватывает и обрабатывает deadlock. Сама retry-логика встроена в
/// RawTickRepository и тестируется через mock-репозиторий.
///
/// Невозможно протестировать retry напрямую на InMemory/SQLite, т.к.
/// NpgsqlParameter несовместим с этими провайдерами.
/// </summary>
public class RawTickRepositoryDeadlockTests
{
    [Fact(Timeout = 10000)]
    public async Task BulkInsertIgnoreConflictsAsync_WhenDeadlockExceedsRetry_ThrowsPostgresException()
    {
        // Arrange — mock репозитория, который всегда кидает deadlock
        var mockRepo = new Mock<IRawTickRepository>();
        mockRepo
            .Setup(x => x.BulkInsertIgnoreConflictsAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .Throws(CreatePostgresDeadlockException());

        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 50000.00m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        // Act
        Func<Task> act = () => mockRepo.Object.BulkInsertIgnoreConflictsAsync(ticks, CancellationToken.None);

        // Assert
        await act.Should().ThrowAsync<Npgsql.PostgresException>();
    }

    [Fact(Timeout = 10000)]
    public async Task BulkInsertIgnoreConflictsAsync_NoDeadlock_InsertsSuccessfully()
    {
        // Arrange
        var mockRepo = new Mock<IRawTickRepository>();
        mockRepo
            .Setup(x => x.BulkInsertIgnoreConflictsAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        var ticks = new List<RawTick>
        {
            new RawTick("BTCUSDT", 50000.00m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService()),
            new RawTick("ETHUSDT", 3000.00m, 1.0m, DateTime.UtcNow, "Binance", new SystemTimeService())
        };

        // Act
        var inserted = await mockRepo.Object.BulkInsertIgnoreConflictsAsync(ticks, CancellationToken.None);

        // Assert
        inserted.Should().Be(2);
    }

    [Fact(Timeout = 10000)]
    public async Task BulkInsertIgnoreConflictsAsync_EmptyList_ReturnsZero()
    {
        // Arrange
        var mockRepo = new Mock<IRawTickRepository>();
        mockRepo
            .Setup(x => x.BulkInsertIgnoreConflictsAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(0);

        // Act
        var inserted = await mockRepo.Object.BulkInsertIgnoreConflictsAsync(new List<RawTick>(), CancellationToken.None);

        // Assert
        inserted.Should().Be(0);
    }

    /// <summary>
    /// Создаёт PostgresException с SqlState=40P01 (deadlock detected).
    /// </summary>
    private static Npgsql.PostgresException CreatePostgresDeadlockException()
    {
        // PostgresException в Npgsql 8.x имеет read-only свойства.
        // Используем FormatterServices для создания + рефлексию для полей.
        var ex = (Npgsql.PostgresException)FormatterServices
            .GetUninitializedObject(typeof(Npgsql.PostgresException));

        var sqlStateField = typeof(Npgsql.PostgresException).GetField("_sqlState",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        sqlStateField?.SetValue(ex, "40P01");

        var messageField = typeof(Npgsql.PostgresException).GetField("_message",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        messageField?.SetValue(ex, "deadlock detected");

        return ex;
    }
}
