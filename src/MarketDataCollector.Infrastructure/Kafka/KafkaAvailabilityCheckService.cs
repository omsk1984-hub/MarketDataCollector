using Confluent.Kafka;
using MarketDataCollector.Core.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Hosted-сервис проверки доступности Kafka при старте.
///
/// Заменяет статический метод VerifyKafkaAvailability (который использовал
/// Console.WriteLine) на корректное структурированное логирование через
/// ILogger. Не бросает исключений — при недоступности Kafka свечи уходят
/// в fallback на прямую запись в БД (решение принимает TickAggregator).
///
/// Выполняет проверку один раз при старте и логирует результат.
/// </summary>
public sealed class KafkaAvailabilityCheckService : IHostedService
{
    private readonly ILogger<KafkaAvailabilityCheckService> _logger;
    private readonly KafkaOptions _options;

    public KafkaAvailabilityCheckService(
        ILogger<KafkaAvailabilityCheckService> logger,
        IOptions<KafkaOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Kafka is disabled. Candle aggregation will write directly to DB.");
            return Task.CompletedTask;
        }

        try
        {
            var testConfig = new AdminClientConfig { BootstrapServers = _options.BootstrapServers };
            using var adminClient = new AdminClientBuilder(testConfig).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

            if (metadata.Brokers.Count > 0)
            {
                var brokers = string.Join(", ", metadata.Brokers.Select(b => $"{b.Host}:{b.Port}"));
                _logger.LogInformation(
                    "Kafka broker(s) available: {Brokers}. Bootstrap={Bootstrap}",
                    brokers, _options.BootstrapServers);
            }
            else
            {
                _logger.LogWarning(
                    "No Kafka brokers found at {Bootstrap}. Candles will fall back to direct DB write until Kafka becomes available.",
                    _options.BootstrapServers);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "Cannot reach Kafka at {Bootstrap}: {Message}. Candles will fall back to direct DB write until Kafka becomes available.",
                _options.BootstrapServers, ex.Message);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
