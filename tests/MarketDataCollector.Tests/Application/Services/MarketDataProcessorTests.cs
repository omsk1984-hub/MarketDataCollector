using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Globalization;
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
    private readonly Mock<IServiceScopeFactory> _scopeFactoryMock;
    private readonly Mock<IServiceScope> _scopeMock;
    private readonly Mock<IServiceProvider> _scopeServiceProviderMock;

    public MarketDataProcessorTests(ITestOutputHelper output)
    {
        _output = output;
        _repositoryMock = new Mock<IRawTickRepository>();
        _loggerMock = new Mock<ILogger<MarketDataProcessor>>();
        _timeServiceMock = new Mock<ITimeService>();

        // Настраиваем IServiceScopeFactory, чтобы она возвращала scope с нужным репозиторием
        _scopeServiceProviderMock = new Mock<IServiceProvider>();
        _scopeServiceProviderMock
            .Setup(sp => sp.GetService(typeof(IRawTickRepository)))
            .Returns(_repositoryMock.Object);

        _scopeMock = new Mock<IServiceScope>();
        _scopeMock.Setup(s => s.ServiceProvider).Returns(_scopeServiceProviderMock.Object);

        _scopeFactoryMock = new Mock<IServiceScopeFactory>();
        _scopeFactoryMock
            .Setup(f => f.CreateScope())
            .Returns(_scopeMock.Object);
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithValidParameters_CreatesProcessor()
    {
        // Arrange & Act
        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 10,
            channelCapacity: 200);

        // Assert
        processor.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithZeroBatchSize_CreatesProcessor()
    {
        // Arrange & Act
        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
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
            _scopeFactoryMock.Object,
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

        // Assert - проверяем, что тик попал в канал
        var reader = processor.Channel.Reader;
        var readTask = reader.ReadAsync().AsTask();
        var completed = await Task.WhenAny(readTask, Task.Delay(1000));
        completed.Should().Be(readTask, "Тик должен быть доступен для чтения из канала");
        var tick = await readTask;
        tick.Ticker.Should().Be(ticker);
        tick.Price.Should().Be(price);
        tick.Volume.Should().Be(volume);
        tick.Exchange.Should().Be(exchange);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessTickAsync_LogsDebugMessage()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_LogsDebugMessage)} ===");
        // Arrange
        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
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
            _scopeFactoryMock.Object,
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
                                                o.ToString()!.Contains(price.ToString(CultureInfo.InvariantCulture)) &&
                                                o.ToString()!.Contains(volume.ToString(CultureInfo.InvariantCulture)) &&
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
            _scopeFactoryMock.Object,
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
            _scopeFactoryMock.Object,
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
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        
        await processor.StartProcessingAsync(cts.Token);

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
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        
        await processor.StartProcessingAsync(cts.Token);
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
            _scopeFactoryMock.Object,
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
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 100);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow, "Binance");

        // Assert - проверяем через канал
        var reader = processor.Channel.Reader;
        var count = 0;
        while (reader.TryRead(out _))
        {
            count++;
        }
        count.Should().Be(3);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_SavesNewTicksToRepository()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_SavesNewTicksToRepository)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Ждём обработки через StopProcessingAsync
        await processor.StopProcessingAsync(cts.Token);

        // Assert - репозиторий вызывается через scope внутри ProcessBatchAsync
        _repositoryMock.Verify(x => x.BulkCopyAsync(
            It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_SkipsDuplicateTicks()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_SkipsDuplicateTicks)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1); // Только один тик реально вставлен (дубликат отброшен БД)

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        var timestamp1 = new DateTime(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var timestamp2 = new DateTime(2024, 1, 1, 10, 0, 1, DateTimeKind.Utc);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act - используем разные timestamp, чтобы они не считались дубликатами в памяти
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp2, "Binance");

        // Ждём обработки через StopProcessingAsync
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _repositoryMock.Verify(x => x.BulkCopyAsync(
            It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsSkippedDuplicates()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsSkippedDuplicates)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(2); // Оба вставлены

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Ждём обработки через StopProcessingAsync
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("дубликатов")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database error"));

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

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

        // Ждём обработки через StopProcessingAsync
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        errorOccurred.Should().BeTrue();
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Критическая ошибка")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsSavedCount()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsSavedCount)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(2);

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 2,
            channelCapacity: 100);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
        await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow, "Binance");

        // Ждём обработки через StopProcessingAsync
        await processor.StopProcessingAsync(cts.Token);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Батч сохранён")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce);
    }

    [Fact(Timeout = 30000)]
    public async Task ProcessBatchAsync_SingleReader_ProcessesBatchesSequentially()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_SingleReader_ProcessesBatchesSequentially)} ===");

        // Arrange
        var concurrentCalls = 0;
        var maxConcurrentCalls = 0;
        var callLock = new object();

        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .Returns< IEnumerable<RawTick>, CancellationToken>(async (ticks, ct) =>
            {
                // Эмулируем длительную DB-операцию (100ms)
                lock (callLock)
                {
                    concurrentCalls++;
                    if (concurrentCalls > maxConcurrentCalls)
                        maxConcurrentCalls = concurrentCalls;
                }

                await Task.Delay(100, ct);

                lock (callLock)
                {
                    concurrentCalls--;
                }

                return ticks.Count();
            });

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 200);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await processor.StartProcessingAsync(cts.Token);

        // Act — отправляем 20 тиков (batchSize=5 => минимум 4 батча)
        for (int i = 0; i < 20; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i, DateTimeKind.Utc),
                "Binance");
        }

        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert — SingleReader=true, только 1 consumer, поэтому
        // BulkInsertIgnoreConflictsAsync вызывается последовательно
        maxConcurrentCalls.Should().Be(1,
            "SingleReader=true гарантирует последовательную запись в БД");
    }

    [Fact(Timeout = 30000)]
    public async Task ProcessBatchAsync_WithAggregator_SingleReaderStillSequential()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_WithAggregator_SingleReaderStillSequential)} ===");

        // Arrange
        var maxConcurrentCalls = 0;
        var concurrentCalls = 0;
        var callLock = new object();

        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .Returns< IEnumerable<RawTick>, CancellationToken>(async (ticks, ct) =>
            {
                lock (callLock)
                {
                    concurrentCalls++;
                    if (concurrentCalls > maxConcurrentCalls)
                        maxConcurrentCalls = concurrentCalls;
                }

                await Task.Delay(50, ct);

                lock (callLock)
                {
                    concurrentCalls--;
                }

                return ticks.Count();
            });

        // Создаём mock агрегатора
        var aggregatorMock = new Mock<ITickAggregator>();
        aggregatorMock
            .Setup(x => x.OnTickAsync(It.IsAny<string>(), It.IsAny<decimal>(), It.IsAny<decimal>(),
                It.IsAny<DateTime>(), It.IsAny<string>()))
            .Returns(Task.CompletedTask);

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 5,
            channelCapacity: 200,
            tickAggregator: aggregatorMock.Object);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await processor.StartProcessingAsync(cts.Token);

        // Act — отправляем 15 тиков => минимум 3 батча
        for (int i = 0; i < 15; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i, DateTimeKind.Utc),
                "Binance");
        }

        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert
        maxConcurrentCalls.Should().Be(1,
            "SingleReader=true гарантирует последовательную запись даже с агрегатором");
    }

    [Fact(Timeout = 30000)]
    public async Task ProcessBatchAsync_DbException_ContinuesProcessing()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_DbException_ContinuesProcessing)} ===");

        // Arrange — первые два вызова кидают исключение, третий успешен
        var callCount = 0;
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .Returns< IEnumerable<RawTick>, CancellationToken>((ticks, ct) =>
            {
                var current = Interlocked.Increment(ref callCount);
                if (current <= 2)
                    throw new InvalidOperationException("DB connection lost");
                return Task.FromResult(ticks.Count());
            });

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 3,
            channelCapacity: 100);

        var errorCount = 0;
        processor.OnError += (_, _) => Interlocked.Increment(ref errorCount);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await processor.StartProcessingAsync(cts.Token);

        // Act — 9 тиков => 3 батча
        for (int i = 0; i < 9; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i, DateTimeKind.Utc),
                "Binance");
        }

        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert — первые два батча (из 3) упали с ошибкой, третий успешен.
        // С SingleReader=true всё последовательно: 2 ошибки, затем 1 успех + финальный flush.
        errorCount.Should().Be(2,
            "первые два батча должны упасть с ошибкой, третий — успешно обработаться");
        // Проверяем, что BulkInsertIgnoreConflictsAsync вызывался минимум 3 раза
        // (2 ошибки + минимум 1 успех + финальный flush)
        _repositoryMock.Verify(
            x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(3));
    }

    [Fact(Timeout = 10000)]
    public async Task ProcessBatchAsync_LogsTotalProcessedEvery100()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessBatchAsync_LogsTotalProcessedEvery100)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 1,
            channelCapacity: 200);

        using var cts = new CancellationTokenSource();
        await processor.StartProcessingAsync(cts.Token);

        // Act - добавляем 100 тиков с разными timestamp
        for (int i = 0; i < 100; i++)
        {
            await processor.ProcessTickAsync("BTCUSDT", 1000.50m + i, 0.5m,
                new DateTime(2024, 1, 1, 10, 0, i % 60, DateTimeKind.Utc), "Binance");
        }

        // Ждём обработки через StopProcessingAsync
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

    [Fact(Timeout = 15000)]
    public async Task ProcessTickAsync_DoesNotBlockWhenAggregatorIsSlow()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_DoesNotBlockWhenAggregatorIsSlow)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        // Создаём mock агрегатора, который работает ОЧЕНЬ МЕДЛЕННО (5 сек на тик).
        // До исправления (await) это заблокировало бы ProcessTickAsync на 5 сек,
        // после исправления (fire-and-forget) ProcessTickAsync возвращается мгновенно.
        var slowAggregatorMock = new Mock<ITickAggregator>();
        slowAggregatorMock
            .Setup(x => x.OnTickAsync(It.IsAny<string>(), It.IsAny<decimal>(), It.IsAny<decimal>(),
                It.IsAny<DateTime>(), It.IsAny<string>()))
            .Returns(async () => await Task.Delay(5000)); // very slow

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 50,
            channelCapacity: 10000,
            tickAggregator: slowAggregatorMock.Object);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await processor.StartProcessingAsync(cts.Token);

        // Act — отправляем 10 тиков, замеряем время
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < 10; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i, DateTimeKind.Utc),
                "Binance");
        }

        stopwatch.Stop();

        // Дожидаемся обработки
        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert
        // Если бы был await агрегатора, 10 тиков * 5 сек = 50+ секунд.
        // С fire-and-forget вызовы возвращаются немедленно.
        // Время должно быть < 1 секунды (только WriteAsync в Channel).
        stopwatch.Elapsed.TotalSeconds.Should().BeLessThan(1.0,
            "ProcessTickAsync не должен ждать агрегатор (fire-and-forget)");

        // Проверяем, что основной пайплайн всё равно обработал тики
        slowAggregatorMock.Verify(
            x => x.OnTickAsync(It.IsAny<string>(), It.IsAny<decimal>(), It.IsAny<decimal>(),
                It.IsAny<DateTime>(), It.IsAny<string>()),
            Times.Exactly(10),
            "Агрегатор должен получить все 10 тиков через fire-and-forget");

        _repositoryMock.Verify(
            x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce,
            "Тики всё равно должны сохраняться в БД через основной пайплайн");
    }

    [Fact(Timeout = 15000)]
    public async Task ProcessTickAsync_DoesNotBlockWhenAggregatorChannelIsFull()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_DoesNotBlockWhenAggregatorChannelIsFull)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        // Создаём реальный TickAggregator с малым каналом (capacity=5, DropOldest).
        // Если ProcessTickAsync блокируется на WriteAsync в канал агрегатора,
        // то после заполнения канала он бы завис. Но fire-and-forget +
        // DropOldest позволяют каналу отбрасывать старые тики без блокировки.
        var aggregator = new TickAggregator(
            _timeServiceMock.Object,
            Mock.Of<ILogger<TickAggregator>>(),
            Mock.Of<IServiceScopeFactory>(),
            Options.Create(new TickAggregatorOptions
            {
                CandleIntervalSeconds = 3600, // никогда не завершается
                FlushIntervalSeconds = 86400, // никогда не флашится
                ChannelCapacity = 5           // очень маленький канал
            }));

        using var aggregatorCts = new CancellationTokenSource();
        await aggregator.StartAsync(aggregatorCts.Token);

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 100,
            channelCapacity: 10000,
            tickAggregator: aggregator);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await processor.StartProcessingAsync(cts.Token);

        // Act — отправляем 1000 тиков. Канал агрегатора вмещает всего 5.
        // Если бы был await, после 5-го тика ProcessTickAsync заблокировался бы.
        // С fire-and-forget + DropOldest — всё проходит быстро.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < 1000; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i % 60, DateTimeKind.Utc),
                "Binance");
        }

        stopwatch.Stop();

        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert — 1000 вызовов должны пройти быстро (<< 1 секунда на вызов)
        stopwatch.Elapsed.TotalSeconds.Should().BeLessThan(5.0,
            "ProcessTickAsync не должен блокироваться при полном канале агрегатора (DropOldest)");

        // Проверяем, что основной пайплайн работал
        var processedCount = await processor.GetProcessedCountAsync();
        _output.WriteLine($"Процессор обработал {processedCount} тиков (из 1000 отправленных)");

        _repositoryMock.Verify(
            x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce,
            "Тики должны сохраняться в БД, независимо от агрегатора");
    }

    [Fact(Timeout = 15000)]
    public async Task ProcessTickAsync_DoesNotBlockWhenMainChannelIsFull()
    {
        _output.WriteLine($"=== Running: {nameof(ProcessTickAsync_DoesNotBlockWhenMainChannelIsFull)} ===");
        // Arrange
        _repositoryMock
            .Setup(x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                // Эмулируем медленный DB write (200ms), чтобы канал быстро заполнился
                await Task.Delay(200);
                return 1;
            });

        var processor = new MarketDataProcessor(
            _scopeFactoryMock.Object,
            _loggerMock.Object,
            _timeServiceMock.Object,
            batchSize: 100,     // большой batch — чтобы consumer не успевал за producer
            channelCapacity: 10); // очень маленький канал (вмещает всего 10 тиков)

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await processor.StartProcessingAsync(cts.Token);

        // Act — отправляем 100 тиков. Канал вмещает 10, consumer медленный (200ms).
        // Если FullMode=Wait, то после 10-го тика WriteAsync заблокируется.
        // С DropOldest — тики отбрасываются, producer не блокируется.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < 100; i++)
        {
            await processor.ProcessTickAsync(
                "BTCUSDT",
                50000.00m + i,
                0.5m,
                new DateTime(2024, 1, 1, 10, 0, i % 60, DateTimeKind.Utc),
                "Binance");
        }

        stopwatch.Stop();

        await processor.StopProcessingAsync(CancellationToken.None);

        // Assert — 100 вызовов с медленным consumer должны пройти быстро.
        // Если бы был Wait, то >10s (пока consumer переварит 100 тиков по 200ms).
        // С DropOldest — все 100 WriteAsync отрабатывают мгновенно.
        stopwatch.Elapsed.TotalSeconds.Should().BeLessThan(2.0,
            "DropOldest не должен блокировать producer при переполнении канала");

        var processedCount = await processor.GetProcessedCountAsync();
        _output.WriteLine($"Процессор обработал {processedCount} тиков (из 100 отправленных, канал теряет часть)");

        _repositoryMock.Verify(
            x => x.BulkCopyAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce,
            "DropOldest не должен останавливать основной пайплайн обработки");
    }
}
