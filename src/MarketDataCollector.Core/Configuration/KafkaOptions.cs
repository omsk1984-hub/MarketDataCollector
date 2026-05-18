namespace MarketDataCollector.Core.Configuration;

public class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>Адрес bootstrap-сервера Kafka.</summary>
    public string BootstrapServers { get; set; } = "localhost:9094";

    /// <summary>ID consumer-группы для aggregated-data.</summary>
    public string AggregatedDataGroupId { get; set; } = "marketdata-aggregated-group";

    /// <summary>Имя топика для агрегированных данных.</summary>
    public string AggregatedDataTopic { get; set; } = "aggregated-data";

    /// <summary>Таймаут подтверждения записи (acks).</summary>
    public int AcksTimeoutMs { get; set; } = 5000;

    /// <summary>Максимальный размер пакета (байт).</summary>
    public int MessageMaxBytes { get; set; } = 1048576;

    /// <summary>Включить/отключить Kafka интеграцию.</summary>
    public bool Enabled { get; set; } = true;
}
