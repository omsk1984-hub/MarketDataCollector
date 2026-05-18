using System.Text.Json;
using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Tests.Infrastructure.Kafka;

public class KafkaIntegrationTests
{
    private static readonly DateTime BaseTime = new(2024, 1, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    #region KafkaCandleProducer Tests

    [Fact(Timeout = 5000)]
    public async Task KafkaCandleProducer_ProduceAsync_CallsUnderlyingProducerWithCorrectTopic()
    {
        // Arrange
        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        var options = Options.Create(new KafkaOptions
        {
            AggregatedDataTopic = "aggregated-data",
            BootstrapServers = "localhost:9094",
            Enabled = true
        });
        var logger = new Mock<ILogger<KafkaCandleProducer>>();
        var producer = new KafkaCandleProducer(mockProducer.Object, options, logger.Object);
        var startTime = BaseTime;
        var endTime = BaseTime.AddMinutes(1);
        string capturedTopic = null!;
        string capturedKey = null!;
        string capturedValue = null!;

        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((topic, key, value, ct) =>
            {
                capturedTopic = topic;
                capturedKey = key;
                capturedValue = value;
            })
            .Returns(Task.CompletedTask);

        // Act
        await producer.ProduceAsync(
            "btcusdt", "1m",
            45000.00m, 45100.00m, 44950.00m, 45080.00m, 123.45m,
            startTime, endTime, "binance");

        // Assert
        capturedTopic.Should().Be("aggregated-data");
        capturedKey.Should().Be("btcusdt:binance");

        // Проверяем, что JSON корректный
        var json = JsonDocument.Parse(capturedValue);
        json.RootElement.GetProperty("ticker").GetString().Should().Be("btcusdt");
        json.RootElement.GetProperty("interval").GetString().Should().Be("1m");
        json.RootElement.GetProperty("open").GetDecimal().Should().Be(45000.00m);
        json.RootElement.GetProperty("high").GetDecimal().Should().Be(45100.00m);
        json.RootElement.GetProperty("low").GetDecimal().Should().Be(44950.00m);
        json.RootElement.GetProperty("close").GetDecimal().Should().Be(45080.00m);
        json.RootElement.GetProperty("volume").GetDecimal().Should().Be(123.45m);
        json.RootElement.GetProperty("exchange").GetString().Should().Be("binance");
        json.RootElement.GetProperty("startTime").GetDateTime().Should().Be(startTime);
        json.RootElement.GetProperty("endTime").GetDateTime().Should().Be(endTime);

        mockProducer.Verify(p => p.ProduceAsync(
            "aggregated-data",
            "btcusdt:binance",
            It.IsAny<string>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact(Timeout = 5000)]
    public void KafkaCandleProducer_Constructor_WithNullProducer_ThrowsArgumentNullException()
    {
        // Arrange
        var options = Options.Create(new KafkaOptions());
        var logger = new Mock<ILogger<KafkaCandleProducer>>();

        // Act
        var act = () => new KafkaCandleProducer(null!, options, logger.Object);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("producer");
    }

    [Fact(Timeout = 5000)]
    public void KafkaCandleProducer_Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        var logger = new Mock<ILogger<KafkaCandleProducer>>();

        // Act
        var act = () => new KafkaCandleProducer(mockProducer.Object, null!, logger.Object);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }

    [Fact(Timeout = 5000)]
    public async Task KafkaCandleProducer_ProduceAsync_WithCancellation_ThrowsOperationCanceled()
    {
        // Arrange
        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        var options = Options.Create(new KafkaOptions());
        var logger = new Mock<ILogger<KafkaCandleProducer>>();
        var producer = new KafkaCandleProducer(mockProducer.Object, options, logger.Object);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        // Act
        var act = async () => await producer.ProduceAsync(
            "btcusdt", "1m", 45000m, 45100m, 44950m, 45080m, 123.45m,
            BaseTime, BaseTime.AddMinutes(1), "binance", cts.Token);

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region IKafkaProducer Mock Verification Tests

    [Fact(Timeout = 5000)]
    public async Task KafkaCandleProducer_ProduceAsync_ReturnsJsonWithAllRequiredFields()
    {
        // Arrange
        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        var options = Options.Create(new KafkaOptions());
        var logger = new Mock<ILogger<KafkaCandleProducer>>();
        var producer = new KafkaCandleProducer(mockProducer.Object, options, logger.Object);
        string? capturedJson = null;

        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((_, _, json, _) => capturedJson = json)
            .Returns(Task.CompletedTask);

        // Act
        await producer.ProduceAsync(
            "ethusdt", "5m",
            3000.00m, 3050.00m, 2980.00m, 3020.50m, 500.75m,
            BaseTime, BaseTime.AddMinutes(5), "binance");

        // Assert
        capturedJson.Should().NotBeNull();
        var doc = JsonDocument.Parse(capturedJson!);
        doc.RootElement.EnumerateObject().Select(p => p.Name)
            .Should().BeEquivalentTo(new[]
            {
                "ticker", "interval", "open", "high", "low",
                "close", "volume", "startTime", "endTime", "exchange"
            });

        // Проверяем формат чисел — должны быть Decimal, не строка
        doc.RootElement.GetProperty("open").ValueKind.Should().Be(JsonValueKind.Number);
        doc.RootElement.GetProperty("high").ValueKind.Should().Be(JsonValueKind.Number);
        doc.RootElement.GetProperty("volume").ValueKind.Should().Be(JsonValueKind.Number);
    }

    #endregion

    #region TickAggregator with Kafka Tests

    [Fact(Timeout = 5000)]
    public async Task TickAggregator_WithKafkaEnabled_PublishesCandlesToKafka()
    {
        // Arrange
        var timeServiceMock = new Mock<ITimeService>();
        timeServiceMock.Setup(t => t.UtcNow).Returns(BaseTime.AddHours(1));

        var loggerMock = new Mock<ILogger<TickAggregator>>();
        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        var options = Options.Create(new TickAggregatorOptions
        {
            CandleIntervalSeconds = 60,
            FlushIntervalSeconds = 1, // Быстрый flush для теста
            ChannelCapacity = 10000
        });

        var kafkaProducerMock = new Mock<IKafkaProducer<string, string>>();
        var kafkaLoggerMock = new Mock<ILogger<KafkaCandleProducer>>();
        var kafkaOptions = Options.Create(new KafkaOptions
        {
            Enabled = true,
            AggregatedDataTopic = "aggregated-data"
        });

        var kafkaCandleProducer = new KafkaCandleProducer(
            kafkaProducerMock.Object, kafkaOptions, kafkaLoggerMock.Object);

        var aggregator = new TickAggregator(
            timeServiceMock.Object,
            loggerMock.Object,
            scopeFactoryMock.Object,
            options,
            kafkaCandleProducer,
            kafkaOptions);

        var publishCount = 0;
        kafkaProducerMock
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => publishCount++)
            .Returns(Task.CompletedTask);

        // Act — отправляем тики, чтобы сформировалась свеча
        await aggregator.StartAsync();
        await aggregator.OnTickAsync("btcusdt", 45000m, 1.0m, BaseTime, "binance");
        await aggregator.OnTickAsync("btcusdt", 45100m, 0.5m, BaseTime.AddSeconds(10), "binance");
        await aggregator.OnTickAsync("btcusdt", 44950m, 2.0m, BaseTime.AddSeconds(20), "binance");

        // Ждём, пока таймер flush'а сработает
        await Task.Delay(1500);

        // Останавливаем агрегатор (вызовет финальный flush всех свечей)
        await aggregator.StopAsync();

        // Assert — свеча была опубликована в Kafka (через финальный flush или таймер)
        publishCount.Should().BeGreaterThanOrEqualTo(1);
        kafkaProducerMock.Verify(
            p => p.ProduceAsync("aggregated-data",
                It.Is<string>(key => key == "btcusdt:binance"),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact(Timeout = 5000)]
    public async Task TickAggregator_WithKafkaDisabled_WritesToDatabase()
    {
        // Arrange
        var timeServiceMock = new Mock<ITimeService>();
        timeServiceMock.Setup(t => t.UtcNow).Returns(BaseTime.AddHours(1));

        var loggerMock = new Mock<ILogger<TickAggregator>>();
        var options = Options.Create(new TickAggregatorOptions
        {
            CandleIntervalSeconds = 60,
            FlushIntervalSeconds = 1,
            ChannelCapacity = 10000
        });

        // Мокаем репозиторий через scope factory
        var repoMock = new Mock<IAggregatedDataRepository>();
        repoMock.Setup(r => r.AddRangeAsync(It.IsAny<IEnumerable<AggregatedData>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repoMock.Setup(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock.Setup(s => s.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);

        var scopeMock = new Mock<IServiceScope>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);

        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        scopeFactoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        // Kafka Options выключены
        var kafkaOptions = Options.Create(new KafkaOptions { Enabled = false });

        // Создаём агрегатор БЕЗ Kafka producer (null)
        var aggregator = new TickAggregator(
            timeServiceMock.Object,
            loggerMock.Object,
            scopeFactoryMock.Object,
            options,
            kafkaCandleProducer: null,
            kafkaOptions: kafkaOptions);

        // Act
        await aggregator.StartAsync();
        await aggregator.OnTickAsync("btcusdt", 45000m, 1.0m, BaseTime, "binance");
        await aggregator.OnTickAsync("btcusdt", 45100m, 0.5m, BaseTime.AddSeconds(10), "binance");

        await Task.Delay(1500);

        await aggregator.StopAsync();

        // Assert — данные были записаны через репозиторий
        repoMock.Verify(r => r.AddRangeAsync(
            It.IsAny<IEnumerable<AggregatedData>>(),
            It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
        repoMock.Verify(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    #endregion

    #region KafkaCandleConsumerService Tests

    [Fact(Timeout = 5000)]
    public void KafkaCandleConsumerService_Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var scopeFactory = new Mock<IServiceScopeFactory>();
        var logger = new Mock<ILogger<KafkaCandleConsumerService>>();

        // Act
        var act = () => new KafkaCandleConsumerService(null!, scopeFactory.Object, logger.Object);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }

    [Fact(Timeout = 5000)]
    public void KafkaCandleConsumerService_Constructor_WithNullScopeFactory_ThrowsArgumentNullException()
    {
        // Arrange
        var options = Options.Create(new KafkaOptions());
        var logger = new Mock<ILogger<KafkaCandleConsumerService>>();

        // Act
        var act = () => new KafkaCandleConsumerService(options, null!, logger.Object);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("scopeFactory");
    }

    [Fact(Timeout = 5000)]
    public void KafkaCandleConsumerService_Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var options = Options.Create(new KafkaOptions());
        var scopeFactory = new Mock<IServiceScopeFactory>();

        // Act
        var act = () => new KafkaCandleConsumerService(options, scopeFactory.Object, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    /// <summary>
    /// Интеграционный тест, который проверяет полный цикл:
    /// KafkaCandleProducer (публикация) -> десериализация -> запись в репозиторий.
    /// Эмулирует consumer без реальной Kafka.
    /// </summary>
    [Fact(Timeout = 5000)]
    public async Task FullCycle_ProduceThroughTickAggregator_ConsumerCanDeserializeAndStore()
    {
        // Arrange
        var timeServiceMock = new Mock<ITimeService>();
        var utcNow = BaseTime.AddHours(1);
        timeServiceMock.Setup(t => t.UtcNow).Returns(utcNow);

        // Канал для передачи JSON между producer'ом и "consumer'ом"
        var producedMessages = new List<(string Key, string Json)>();

        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((topic, key, json, ct) =>
            {
                producedMessages.Add((key, json));
            })
            .Returns(Task.CompletedTask);

        var kafkaOptions = Options.Create(new KafkaOptions
        {
            Enabled = true,
            AggregatedDataTopic = "aggregated-data",
            AggregatedDataGroupId = "test-group"
        });
        var kafkaLogger = new Mock<ILogger<KafkaCandleProducer>>();
        var kafkaCandleProducer = new KafkaCandleProducer(
            mockProducer.Object, kafkaOptions, kafkaLogger.Object);

        var aggOptions = Options.Create(new TickAggregatorOptions
        {
            CandleIntervalSeconds = 60,
            FlushIntervalSeconds = 1,
            ChannelCapacity = 10000
        });

        var aggregator = new TickAggregator(
            timeServiceMock.Object,
            new Mock<ILogger<TickAggregator>>().Object,
            new Mock<IServiceScopeFactory>().Object,
            aggOptions,
            kafkaCandleProducer,
            kafkaOptions);

        // Мокаем репозиторий для consumer'а
        var repoMock = new Mock<IAggregatedDataRepository>();
        repoMock.Setup(r => r.AddAsync(It.IsAny<AggregatedData>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        repoMock.Setup(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        var serviceProviderMock = new Mock<IServiceProvider>();
        serviceProviderMock.Setup(s => s.GetService(typeof(IAggregatedDataRepository)))
            .Returns(repoMock.Object);
        serviceProviderMock.Setup(s => s.GetService(typeof(ITimeService)))
            .Returns(timeServiceMock.Object);

        var scopeMock = new Mock<IServiceScope>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);

        var scopeFactoryMock = new Mock<IServiceScopeFactory>();
        scopeFactoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        // Act — producer side: формируем свечи через TickAggregator
        await aggregator.StartAsync();
        await aggregator.OnTickAsync("btcusdt", 45000m, 1.0m, BaseTime, "binance");
        await aggregator.OnTickAsync("btcusdt", 45100m, 0.5m, BaseTime.AddSeconds(10), "binance");
        await aggregator.OnTickAsync("btcusdt", 44950m, 2.0m, BaseTime.AddSeconds(20), "binance");
        await Task.Delay(1500);
        await aggregator.StopAsync();

        // Проверяем, что сообщения были опубликованы
        producedMessages.Should().NotBeEmpty();

        // Act — consumer side: симулируем то, что делает KafkaCandleConsumerService
        foreach (var (key, json) in producedMessages)
        {
            // 1. Десериализуем JSON
            var msg = JsonSerializer.Deserialize<TestCandleMessage>(json, JsonOptions);
            msg.Should().NotBeNull();
            msg!.Ticker.Should().Be("btcusdt");
            msg.Exchange.Should().Be("binance");
            msg.Interval.Should().Be("1m");
            msg.Open.Should().Be(45000m);
            msg.High.Should().BeGreaterThanOrEqualTo(msg.Low);

            // 2. Создаём сущность AggregatedData
            var entity = new AggregatedData(
                msg.Ticker, msg.Interval,
                msg.Open, msg.High, msg.Low, msg.Close, msg.Volume,
                msg.StartTime, msg.EndTime,
                timeServiceMock.Object);

            // 3. Сохраняем в репозиторий
            await repoMock.Object.AddAsync(entity);
            await repoMock.Object.SaveChangesAsync();
        }

        // Assert — consumer успешно сохранил данные
        repoMock.Verify(r => r.AddAsync(
            It.Is<AggregatedData>(a =>
                a.Ticker == "btcusdt" &&
                a.Interval == "1m" &&
                a.OpenPrice == 45000m &&
                a.ClosePrice == 44950m),
            It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        repoMock.Verify(r => r.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);

        // Проверяем, что ключ сообщения совпадает с ожидаемым
        producedMessages.Should().Contain(m => m.Key == "btcusdt:binance");
    }

    #endregion

    #region JSON Serialization Contract Tests

    [Fact(Timeout = 5000)]
    public async Task CandleMessage_Serialization_MatchesConsumerDeserialization()
    {
        // Arrange
        var mockProducer = new Mock<IKafkaProducer<string, string>>();
        var options = Options.Create(new KafkaOptions());
        var logger = new Mock<ILogger<KafkaCandleProducer>>();
        var producer = new KafkaCandleProducer(mockProducer.Object, options, logger.Object);
        string? capturedJson = null;

        mockProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, string, CancellationToken>((_, _, json, _) => capturedJson = json)
            .Returns(Task.CompletedTask);

        var startTime = new DateTime(2024, 6, 15, 12, 30, 0, DateTimeKind.Utc);
        var endTime = startTime.AddMinutes(1);

        // Act
        await producer.ProduceAsync(
            "solusdt", "1m",
            150.25m, 155.00m, 148.50m, 153.75m, 10000.00m,
            startTime, endTime, "binance");

        // Assert — проверяем, что consumer сможет десериализовать
        capturedJson.Should().NotBeNull();

        // Consumer-side десериализация
        var deserialized = JsonSerializer.Deserialize<TestCandleMessage>(capturedJson!, JsonOptions);
        deserialized.Should().NotBeNull();
        deserialized!.Ticker.Should().Be("solusdt");
        deserialized.Interval.Should().Be("1m");
        deserialized.Open.Should().Be(150.25m);
        deserialized.High.Should().Be(155.00m);
        deserialized.Low.Should().Be(148.50m);
        deserialized.Close.Should().Be(153.75m);
        deserialized.Volume.Should().Be(10000.00m);
        deserialized.StartTime.Should().Be(startTime);
        deserialized.EndTime.Should().Be(endTime);
        deserialized.Exchange.Should().Be("binance");

        // Проверяем OHLCV-инварианты
        deserialized.High.Should().BeGreaterOrEqualTo(deserialized.Low);
        deserialized.High.Should().BeGreaterOrEqualTo(deserialized.Open);
        deserialized.High.Should().BeGreaterOrEqualTo(deserialized.Close);
        deserialized.Low.Should().BeLessOrEqualTo(deserialized.Open);
        deserialized.Low.Should().BeLessOrEqualTo(deserialized.Close);
        deserialized.Volume.Should().BePositive();
    }

    #endregion

    /// <summary>
    /// Вспомогательная модель для десериализации в тестах consumer'а.
    /// Должна совпадать с CandleMessage в KafkaCandleProducer.
    /// </summary>
    private class TestCandleMessage
    {
        public string Ticker { get; set; } = null!;
        public string Interval { get; set; } = null!;
        public decimal Open { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Close { get; set; }
        public decimal Volume { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public string Exchange { get; set; } = null!;
    }
}
