using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace MarketDataCollector.Tests.Infrastructure.Kafka;

/// <summary>
/// Интеграционные тесты, поднимающие реальный Kafka в Docker-контейнере
/// через Testcontainers. Настройки (топик, groupId) берутся из appsettings.json
/// через стандартный IOptions pipeline.
/// </summary>
public class KafkaRealConnectionTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer;
    private KafkaOptions _kafkaOptions = null!;
    private readonly ILoggerFactory _loggerFactory;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public KafkaRealConnectionTests()
    {
        // Поднимаем контейнер Kafka
        _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.7.0")
            .WithName($"marketdata-kafka-test-{Guid.NewGuid():N}")
            .Build();

        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
        });
    }

    public async Task InitializeAsync()
    {
        // Стартуем Kafka контейнер
        await _kafkaContainer.StartAsync();

        // Загружаем настройки Kafka из конфигурационного файла проекта
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
            .Build();

        var kafkaSection = configuration.GetSection(KafkaOptions.SectionName);
        _kafkaOptions = kafkaSection.Get<KafkaOptions>() ?? new KafkaOptions();

        // Переопределяем BootstrapServers на адрес тестового контейнера
        _kafkaOptions.BootstrapServers = _kafkaContainer.GetBootstrapAddress();

        // Создаём топик с помощью утилиты из контейнера (админка)
        await CreateTopicAsync(_kafkaOptions.AggregatedDataTopic, 3);

        Console.WriteLine($"[TestSetup] Kafka container ready at {_kafkaOptions.BootstrapServers}");
        Console.WriteLine($"[TestSetup] Topic: {_kafkaOptions.AggregatedDataTopic}");
        Console.WriteLine($"[TestSetup] GroupId: {_kafkaOptions.AggregatedDataGroupId}");
    }

    public async Task DisposeAsync()
    {
        _loggerFactory.Dispose();
        await _kafkaContainer.DisposeAsync();
    }

    [Fact(Timeout = 60_000)]
    public async Task KafkaRealConnection_ProduceAndConsume_FullCycle()
    {
        // Arrange: создаём producer и consumer с реальными настройками
        var producerLogger = _loggerFactory.CreateLogger<KafkaProducer>();
        var producer = new KafkaProducer(_kafkaOptions, producerLogger);

        var consumerOptions = Options.Create(_kafkaOptions);
        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        var consumerLoggerMock = new Mock<ILogger<KafkaCandleConsumerService>>();

        // Мокаем репозиторий и timeService для consumer'а
        var timeServiceMock = new Mock<ITimeService>();
        var utcNow = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        timeServiceMock.Setup(t => t.UtcNow).Returns(utcNow);

        var savedCandles = new List<AggregatedData>();

        var repoMock = new Mock<IAggregatedDataRepository>();
        repoMock
            .Setup(r => r.AddAsync(It.IsAny<AggregatedData>(), It.IsAny<CancellationToken>()))
            .Callback<AggregatedData, CancellationToken>((entity, _) => savedCandles.Add(entity))
            .Returns(Task.CompletedTask);
        repoMock
            .Setup(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        // Настраиваем ServiceProvider для consumer'а
        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(s => s.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);
        serviceProviderMock
            .Setup(s => s.GetService(typeof(ITimeService)))
            .Returns(timeServiceMock.Object);

        var scopeMock = new Mock<IServiceScope>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);
        scopeFactoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        // Act — Producer
        const string testKey = "btcusdt:binance";
        const string testTicker = "btcusdt";
        var startTime = new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var endTime = startTime.AddMinutes(1);

        var candleJson = JsonSerializer.Serialize(new
        {
            ticker = testTicker,
            interval = "1m",
            open = 45000.00m,
            high = 45100.00m,
            low = 44950.00m,
            close = 45080.00m,
            volume = 123.45m,
            startTime,
            endTime,
            exchange = "binance"
        }, JsonOptions);

        // Публикуем свечу в реальный Kafka
        await producer.ProduceAsync(
            _kafkaOptions.AggregatedDataTopic,
            testKey,
            candleJson);

        // Flush для гарантии доставки
        await producer.FlushAsync();

        Console.WriteLine($"[Test] Candle published: key={testKey}");

        // Act — Consumer: создаём и запускаем consumer на короткое время
        var consumer = new KafkaCandleConsumerService(
            consumerOptions,
            scopeFactoryMock.Object,
            consumerLoggerMock.Object);

        // Запускаем consumer и ждём, пока он прочитает сообщение
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await consumer.StartAsync(cts.Token);

        // Даём consumer'у время на чтение и запись
        await Task.Delay(3000, CancellationToken.None);

        // Останавливаем consumer
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        savedCandles.Should().NotBeEmpty("Consumer должен был прочитать хотя бы одну свечу");
        savedCandles.Should().ContainSingle();

        var saved = savedCandles[0];
        saved.Ticker.Should().Be(testTicker);
        saved.Interval.Should().Be("1m");
        saved.OpenPrice.Should().Be(45000.00m);
        saved.HighPrice.Should().Be(45100.00m);
        saved.LowPrice.Should().Be(44950.00m);
        saved.ClosePrice.Should().Be(45080.00m);
        saved.Volume.Should().Be(123.45m);
        saved.StartTime.Should().Be(startTime);
        saved.EndTime.Should().Be(endTime);

        Console.WriteLine($"[Test] Candle consumed and saved: ticker={saved.Ticker}, " +
                          $"O={saved.OpenPrice}/H={saved.HighPrice}/L={saved.LowPrice}/C={saved.ClosePrice}");
    }

    [Fact(Timeout = 60_000)]
    public async Task KafkaRealConnection_MultipleCandles_AllConsumed()
    {
        // Arrange: публикуем несколько свечей и проверяем, что все доставлены
        var producerLogger = _loggerFactory.CreateLogger<KafkaProducer>();
        var producer = new KafkaProducer(_kafkaOptions, producerLogger);

        var consumerOptions = Options.Create(_kafkaOptions);
        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        var consumerLoggerMock = new Mock<ILogger<KafkaCandleConsumerService>>();

        var timeServiceMock = new Mock<ITimeService>();
        timeServiceMock.Setup(t => t.UtcNow).Returns(DateTime.UtcNow);

        var savedCandles = new List<AggregatedData>();
        var saveSemaphore = new SemaphoreSlim(0, 100);

        var repoMock = new Mock<IAggregatedDataRepository>();
        repoMock
            .Setup(r => r.AddAsync(It.IsAny<AggregatedData>(), It.IsAny<CancellationToken>()))
            .Callback<AggregatedData, CancellationToken>((entity, _) =>
            {
                savedCandles.Add(entity);
                saveSemaphore.Release();
            })
            .Returns(Task.CompletedTask);
        repoMock
            .Setup(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(s => s.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);
        serviceProviderMock
            .Setup(s => s.GetService(typeof(ITimeService)))
            .Returns(timeServiceMock.Object);

        var scopeMock = new Mock<IServiceScope>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);
        scopeFactoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        // Act — публикуем 5 свечей для разных символов
        var baseTime = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var symbols = new[] { "btcusdt", "ethusdt", "solusdt", "xrpusdt", "adausdt" };

        for (int i = 0; i < symbols.Length; i++)
        {
            var start = baseTime.AddMinutes(i);
            var json = JsonSerializer.Serialize(new
            {
                ticker = symbols[i],
                interval = "1m",
                open = 45000m + i * 100,
                high = 45100m + i * 100,
                low = 44950m + i * 100,
                close = 45080m + i * 100,
                volume = 100m + i * 10,
                startTime = start,
                endTime = start.AddMinutes(1),
                exchange = "binance"
            }, JsonOptions);

            await producer.ProduceAsync(
                _kafkaOptions.AggregatedDataTopic,
                $"{symbols[i]}:binance",
                json);
        }

        await producer.FlushAsync();
        Console.WriteLine($"[Test] Published {symbols.Length} candles");

        // Запускаем consumer
        var consumer = new KafkaCandleConsumerService(
            consumerOptions,
            scopeFactoryMock.Object,
            consumerLoggerMock.Object);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await consumer.StartAsync(cts.Token);

        // Ждём, пока все 5 свечей будут сохранены
        var allSaved = await saveSemaphore.WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(2000, CancellationToken.None);
        await consumer.StopAsync(CancellationToken.None);

        // Assert
        savedCandles.Should().HaveCount(symbols.Length,
            $"все {symbols.Length} свечей должны быть прочитаны и сохранены");

        savedCandles.Select(c => c.Ticker).Should().BeEquivalentTo(symbols);
        savedCandles.Select(c => c.OpenPrice).Should().OnlyHaveUniqueItems(
            "каждая свеча должна иметь свою цену открытия");

        foreach (var candle in savedCandles)
        {
            candle.HighPrice.Should().BeGreaterOrEqualTo(candle.LowPrice,
                $"OHLCV-инвариант для {candle.Ticker}: High >= Low");
            candle.Volume.Should().BePositive();
        }

        Console.WriteLine($"[Test] Successfully consumed {savedCandles.Count} candles: " +
                          $"[{string.Join(", ", savedCandles.Select(c => c.Ticker))}]");
    }

    [Fact(Timeout = 30_000)]
    public async Task KafkaRealConnection_ProducerConsumer_GracefulShutdown()
    {
        // Arrange: проверяем, что producer и consumer корректно завершают работу
        var producerLogger = _loggerFactory.CreateLogger<KafkaProducer>();
        var producer = new KafkaProducer(_kafkaOptions, producerLogger);

        var consumerOptions = Options.Create(_kafkaOptions);
        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        var consumerLoggerMock = new Mock<ILogger<KafkaCandleConsumerService>>();

        var repoMock = new Mock<IAggregatedDataRepository>();
        var timeServiceMock = new Mock<ITimeService>();

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock
            .Setup(s => s.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);
        serviceProviderMock
            .Setup(s => s.GetService(typeof(ITimeService)))
            .Returns(timeServiceMock.Object);

        var scopeMock = new Mock<IServiceScope>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);
        scopeFactoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        var consumer = new KafkaCandleConsumerService(
            consumerOptions,
            scopeFactoryMock.Object,
            consumerLoggerMock.Object);

        // Act: запускаем и сразу останавливаем consumer (graceful shutdown)
        using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await consumer.StartAsync(startCts.Token);

        // Публикуем сообщение перед остановкой
        var candleJson = JsonSerializer.Serialize(new
        {
            ticker = "btcusdt",
            interval = "1m",
            open = 45000m, high = 45100m, low = 44950m, close = 45080m,
            volume = 123.45m,
            startTime = DateTime.UtcNow,
            endTime = DateTime.UtcNow.AddMinutes(1),
            exchange = "binance"
        }, JsonOptions);

        await producer.ProduceAsync(_kafkaOptions.AggregatedDataTopic, "btcusdt:binance", candleJson);
        await producer.FlushAsync();

        await Task.Delay(2000);
        await consumer.StopAsync(CancellationToken.None);

        // Проверяем, что dispose producer'а не падает
        var disposeAction = async () => await ((KafkaProducer)producer).DisposeAsync();
        await disposeAction.Should().NotThrowAsync("Dispose producer должен проходить без ошибок");

        Console.WriteLine("[Test] Graceful shutdown completed without errors");
    }

    #region Тесты на конфигурацию (настройки из appsettings.json)

    [Fact]
    public void KafkaOptions_FromAppSettings_LoadsCorrectValues()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        // Act
        var kafkaOptions = configuration
            .GetSection(KafkaOptions.SectionName)
            .Get<KafkaOptions>();

        // Assert
        kafkaOptions.Should().NotBeNull();
        kafkaOptions!.Enabled.Should().BeTrue();
        kafkaOptions.BootstrapServers.Should().Be("localhost:9094");
        kafkaOptions.AggregatedDataTopic.Should().Be("aggregated-data");
        kafkaOptions.AggregatedDataGroupId.Should().Be("marketdata-aggregated-group");
        kafkaOptions.AcksTimeoutMs.Should().Be(5000);
        kafkaOptions.MessageMaxBytes.Should().Be(1048576);
    }

    [Fact]
    public void KafkaOptions_FromAppSettings_CanBindViaOptionsPattern()
    {
        // Arrange — эмулируем стандартный IOptions<T> pipeline
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .Build();

        var services = new ServiceCollection();
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var kafkaOptions = serviceProvider
            .GetRequiredService<IOptions<KafkaOptions>>()
            .Value;

        // Assert
        kafkaOptions.Should().NotBeNull();
        kafkaOptions.Enabled.Should().BeTrue();
        kafkaOptions.BootstrapServers.Should().Be("localhost:9094");
        kafkaOptions.AggregatedDataTopic.Should().Be("aggregated-data");
        kafkaOptions.AggregatedDataGroupId.Should().Be("marketdata-aggregated-group");
        kafkaOptions.AcksTimeoutMs.Should().Be(5000);
        kafkaOptions.MessageMaxBytes.Should().Be(1048576);
    }

    #endregion

    /// <summary>
    /// Создаёт топик в Kafka через AdminClient.
    /// </summary>
    private async Task CreateTopicAsync(string topicName, int partitions)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            });
            Console.WriteLine($"[TestSetup] Topic '{topicName}' created with {partitions} partitions");
        }
        catch (CreateTopicsException ex) when (ex.Error.Code == ErrorCode.TopicAlreadyExists)
        {
            Console.WriteLine($"[TestSetup] Topic '{topicName}' already exists");
        }
    }
}
