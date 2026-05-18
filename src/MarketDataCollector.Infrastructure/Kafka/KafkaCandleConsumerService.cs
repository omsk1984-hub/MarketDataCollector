using System.Text.Json;
using Confluent.Kafka;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Фоновый сервис-consumer для чтения OHLCV-свечей из Kafka (topic aggregated-data)
/// и записи их в PostgreSQL через IAggregatedDataRepository.
/// 
/// Гарантия доставки: at-least-once.
/// Offset коммитится только после успешной записи в БД.
/// </summary>
public class KafkaCandleConsumerService : IHostedService, IAsyncDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaCandleConsumerService> _logger;
    private readonly KafkaOptions _options;
    private CancellationTokenSource? _cts;
    private Task? _consumingTask;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public KafkaCandleConsumerService(
        IOptions<KafkaOptions> options,
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaCandleConsumerService> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.AggregatedDataGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false,
            SessionTimeoutMs = 30000,
            MaxPollIntervalMs = 300000, // 5 минут на обработку батча
            FetchMaxBytes = _options.MessageMaxBytes,
            AllowAutoCreateTopics = false
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka consumer error: {Code} {Reason}", error.Code, error.Reason);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Kafka consumer assigned partitions: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogWarning(
                    "Kafka consumer partitions revoked: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
            })
            .Build();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("KafkaCandleConsumerService is disabled (Enabled=false)");
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        // Важно: оборачиваем в Task.Run, т.к. _consumer.Consume() — синхронный блокирующий вызов.
        // Без этого StartAsync заблокируется на Consume() и не вернёт управление хосту,
        // что помешает запуску других HostedService (например, Worker).
        _consumingTask = Task.Run(() => ConsumeLoopAsync(_cts.Token), _cts.Token);

        _logger.LogInformation(
            "KafkaCandleConsumerService started. Topic={Topic}, GroupId={Group}",
            _options.AggregatedDataTopic, _options.AggregatedDataGroupId);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts == null) return;

        _logger.LogInformation("KafkaCandleConsumerService stopping...");

        _cts.Cancel();

        try
        {
            if (_consumingTask != null)
            {
                await _consumingTask.WaitAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо при остановке
        }
        finally
        {
            try
            {
                _consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error closing Kafka consumer");
            }
        }

        _logger.LogInformation("KafkaCandleConsumerService stopped");
    }

    /// <summary>
    /// Основной цикл потребления сообщений из Kafka.
    /// Каждое сообщение (свеча) десериализуется и записывается в БД.
    /// Offset коммитится после успешной записи.
    /// </summary>
    private async Task ConsumeLoopAsync(CancellationToken ct)
    {
        _consumer.Subscribe(_options.AggregatedDataTopic);

        _logger.LogInformation(
            "Subscribed to topic {Topic}. Waiting for messages...",
            _options.AggregatedDataTopic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(ct);

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                        continue;

                    await ProcessCandleMessageAsync(consumeResult.Message, ct);

                    // Ручной commit offset'а — только после успешной записи в БД
                    _consumer.Commit(consumeResult);

                    _logger.LogTrace(
                        "Candle consumed and saved. Offset={Offset}, Partition={Partition}, Key={Key}",
                        consumeResult.Offset, consumeResult.Partition, consumeResult.Message.Key);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex,
                        "Kafka consume error. Topic={Topic}, ErrorCode={ErrorCode}",
                        _options.AggregatedDataTopic, ex.Error.Code);

                    // Пауза перед повторной попыткой, чтобы избежать spin loop
                    await Task.Delay(1000, ct);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо при остановке
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Unexpected error in Kafka consumer loop");
        }
    }

    /// <summary>
    /// Обработка одного сообщения-свечи: десериализация + запись в БД.
    /// </summary>
    private async Task ProcessCandleMessageAsync(Message<string, string> message, CancellationToken ct)
    {
        CandleMessage? candle;
        try
        {
            candle = JsonSerializer.Deserialize<CandleMessage>(message.Value, JsonOptions);
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex,
                "Failed to deserialize candle message. Key={Key}, Value={Value}",
                message.Key, message.Value);
            // Не выкидываем исключение — сообщение пропускаем (коммит offset'а будет)
            return;
        }

        if (candle == null)
        {
            _logger.LogWarning("Null candle after deserialization. Key={Key}", message.Key);
            return;
        }

        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();
        var timeService = scope.ServiceProvider.GetRequiredService<ITimeService>();

        var entity = new AggregatedData(
            candle.Ticker,
            candle.Interval,
            candle.Open,
            candle.High,
            candle.Low,
            candle.Close,
            candle.Volume,
            candle.StartTime,
            candle.EndTime,
            timeService);

        // Используем существующую логику репозитория для записи
        await repository.AddAsync(entity, ct);
        await repository.SaveChangesAsync(ct);

        _logger.LogDebug(
            "Candle saved to DB. Ticker={Ticker}, Interval={Interval}, " +
            "O={Open}/H={High}/L={Low}/C={Close}, Start={Start}",
            candle.Ticker, candle.Interval, candle.Open, candle.High,
            candle.Low, candle.Close, candle.StartTime);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Dispose();
        _consumer?.Dispose();
        await Task.CompletedTask;
    }

    /// <summary>
    /// Внутренняя модель сообщения для десериализации.
    /// </summary>
    private class CandleMessage
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
