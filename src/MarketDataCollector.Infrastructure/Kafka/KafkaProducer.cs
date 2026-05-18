using System.Text.Json;
using Confluent.Kafka;
using MarketDataCollector.Core.Configuration;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Базовая реализация Kafka producer'а через Confluent.Kafka.
/// Использует JSON-сериализацию и настройку acks=all для гарантии доставки.
/// </summary>
public class KafkaProducer : IKafkaProducer<string, string>, IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly KafkaOptions _options;
    private bool _disposed;

    public KafkaProducer(KafkaOptions options, ILogger<KafkaProducer> logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            MessageTimeoutMs = options.AcksTimeoutMs,
            MessageMaxBytes = options.MessageMaxBytes,
            EnableIdempotence = true,
            CompressionType = CompressionType.Snappy,
            LingerMs = 5,
            BatchSize = 65536 // 64KB
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka producer error: {Code} {Reason}", error.Code, error.Reason);
            })
            .SetStatisticsHandler((_, json) =>
            {
                _logger.LogDebug("Kafka producer statistics: {Stats}", json);
            })
            .Build();

        _logger.LogInformation(
            "KafkaProducer created. BootstrapServers={Bootstrap}, Topic={Topic}",
            options.BootstrapServers, options.AggregatedDataTopic);
    }

    /// <inheritdoc />
    public async Task ProduceAsync(
        string topic, string key, string value,
        CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(KafkaProducer));

        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = new Headers
                {
                    new Header("content-type", System.Text.Encoding.UTF8.GetBytes("application/json")),
                    new Header("timestamp", System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")))
                }
            };

            var deliveryResult = await _producer.ProduceAsync(topic, message, cancellationToken);

            if (deliveryResult.Status != PersistenceStatus.Persisted)
            {
                _logger.LogWarning(
                    "Message to topic {Topic} partition {Partition} offset {Offset} status: {Status}",
                    deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset, deliveryResult.Status);
            }
            else
            {
                _logger.LogTrace(
                    "Message delivered to topic {Topic} partition {Partition} offset {Offset}",
                    deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset);
            }
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Failed to produce message to topic {Topic}. Error: {ErrorCode} {ErrorReason}",
                topic, ex.Error.Code, ex.Error.Reason);
            throw;
        }
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        _producer.Flush(cancellationToken);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        try
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error flushing Kafka producer during dispose");
        }

        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}
