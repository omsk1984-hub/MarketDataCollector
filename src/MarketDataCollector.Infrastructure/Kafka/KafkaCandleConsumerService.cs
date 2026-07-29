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

    /// Минимальная задержка между повторными попытками (при ошибке broker).
    private static readonly TimeSpan InitialBackoff = TimeSpan.FromSeconds(1);
    /// Максимальная задержка между повторными попытками.
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);
    /// Счётчик последовательных ошибок для экспоненциального backoff.
    private int _consecutiveErrors;

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
                    FormatPartitions(partitions.Select(p => (p.Topic, p.Partition.Value))));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogWarning(
                    "Kafka consumer partitions revoked: {Partitions}",
                    FormatPartitions(partitions.Select(p => (p.Topic, p.Partition.Value))));
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
                // Используем таймаут, чтобы гарантированно завершить остановку,
                // даже если Consumer.Consume() не реагирует на отмену токена
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(10));
                
                await _consumingTask.WaitAsync(timeoutCts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Ожидаемо при остановке или таймауте
            _logger.LogWarning("Consumer task did not complete within timeout, forcing close");
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
    /// При ошибках broker используется экспоненциальный backoff.
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
                    // Используем Consume с таймаутом вместо CancellationToken,
                    // чтобы избежать Access Violation при закрытии consumer:
                    // нативный rd_kafka_consumer_poll не реагирует на .NET CancellationToken,
                    // и вызов _consumer.Close() во время активного poll приводит к
                    // crash (gc handle уничтожается пока ещё используется).
                    // С таймаутом poll завершается максимум за pollTimeout секунд,
                    // после чего безопасно проверяем токен отмены.
                    var pollTimeout = TimeSpan.FromSeconds(1);
                    var consumeResult = _consumer.Consume(pollTimeout);

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                        continue;

                    await ProcessCandleMessageAsync(consumeResult.Message, ct);

                    // Ручной commit offset'а — только после успешной записи в БД
                    _consumer.Commit(consumeResult);

                    // Сброс счётчика ошибок при успешном потреблении
                    Interlocked.Exchange(ref _consecutiveErrors, 0);

                    _logger.LogTrace(
                        "Candle consumed and saved. Offset={Offset}, Partition={Partition}, Key={Key}",
                        consumeResult.Offset, consumeResult.Partition, consumeResult.Message.Key);
                }
                catch (ConsumeException ex)
                {
                    var errorCount = Interlocked.Increment(ref _consecutiveErrors);
                    var delay = CalculateBackoff(errorCount);

                    _logger.LogError(ex,
                        "Kafka consume error (attempt #{Attempt}, next retry in {Delay}s). Topic={Topic}, ErrorCode={ErrorCode}",
                        errorCount, (int)delay.TotalSeconds, _options.AggregatedDataTopic, ex.Error.Code);

                    await Task.Delay(delay, ct);
                }
                catch (Exception ex)
                {
                    var errorCount = Interlocked.Increment(ref _consecutiveErrors);
                    var delay = CalculateBackoff(errorCount);

                    _logger.LogError(ex,
                        "Unexpected error in consumer loop (attempt #{Attempt}, next retry in {Delay}s)",
                        errorCount, (int)delay.TotalSeconds);

                    await Task.Delay(delay, ct);
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
    /// Вычисляет задержку с экспоненциальным backoff: 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
    /// </summary>
    private static TimeSpan CalculateBackoff(int attempt)
    {
        var seconds = Math.Min(InitialBackoff.TotalSeconds * Math.Pow(2, attempt - 1), MaxBackoff.TotalSeconds);
        return TimeSpan.FromSeconds(seconds);
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
    /// Форматирует список партиций в компактный вид: topic[p0, p1, p2], topic2[p0]
    /// Группирует по топику, чтобы избежать повторения имени топика для каждой партиции.
    /// </summary>
    private static string FormatPartitions(IEnumerable<(string Topic, int Partition)> partitions)
    {
        return string.Join(", ",
            partitions
                .GroupBy(p => p.Topic)
                .Select(g => $"{g.Key}[{string.Join(", ", g.Select(p => p.Partition))}]"));
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
