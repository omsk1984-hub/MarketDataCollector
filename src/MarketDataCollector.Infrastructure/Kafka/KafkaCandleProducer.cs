using System.Text.Json;
using MarketDataCollector.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Producer для отправки OHLCV-свечей (AggregatedData) в Kafka topic aggregated-data.
/// </summary>
public class KafkaCandleProducer
{
    private readonly IKafkaProducer<string, string> _producer;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaCandleProducer> _logger;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public KafkaCandleProducer(
        IKafkaProducer<string, string> producer,
        IOptions<KafkaOptions> options,
        ILogger<KafkaCandleProducer> logger)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Отправить свечу в Kafka.
    /// </summary>
    public async Task ProduceAsync(
        string ticker,
        string interval,
        decimal open,
        decimal high,
        decimal low,
        decimal close,
        decimal volume,
        DateTime startTime,
        DateTime endTime,
        string exchange,
        CancellationToken cancellationToken = default)
    {
        var key = $"{ticker}:{exchange}";

        var candleMessage = new CandleMessage
        {
            Ticker = ticker,
            Interval = interval,
            Open = open,
            High = high,
            Low = low,
            Close = close,
            Volume = volume,
            StartTime = startTime,
            EndTime = endTime,
            Exchange = exchange
        };

        var json = JsonSerializer.Serialize(candleMessage, JsonOptions);

        await _producer.ProduceAsync(_options.AggregatedDataTopic, key, json, cancellationToken);

        _logger.LogDebug(
            "Candle published to Kafka. Ticker={Ticker}, Interval={Interval}, " +
            "O={Open}/H={High}/L={Low}/C={Close}, V={Volume}, Start={Start}",
            ticker, interval, open, high, low, close, volume, startTime);
    }

    /// <summary>
    /// Внутренняя модель сообщения для сериализации в JSON.
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
