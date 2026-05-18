namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Generic Kafka producer для отправки сообщений.
/// </summary>
public interface IKafkaProducer<TKey, TValue>
{
    /// <summary>Отправить сообщение в топик.</summary>
    Task ProduceAsync(string topic, TKey key, TValue value, CancellationToken cancellationToken = default);

    /// <summary>Flush и освобождение ресурсов.</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}
