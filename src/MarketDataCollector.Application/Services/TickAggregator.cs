using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TickData = MarketDataCollector.Domain.Entities.TickData;

namespace MarketDataCollector.Application.Services
{
    /// <summary>
    /// Композитный ключ для _activeCandles.
    /// Value-type, избегает аллокации строки при каждом тике.
    /// </summary>
    internal readonly record struct AggregatorKey(string Ticker, string Exchange, long BucketTicks)
        : IEquatable<AggregatorKey>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return HashCode.Combine(
                Ticker?.GetHashCode(StringComparison.Ordinal) ?? 0,
                Exchange?.GetHashCode(StringComparison.Ordinal) ?? 0,
                BucketTicks);
        }
    }

    /// <summary>
    /// Агрегатор тиковых данных в OHLCV-свечи заданного интервала.
    /// Работает независимо от основного пайплайна записи RawTicks.
    /// </summary>
    public class TickAggregator : ITickAggregator
    {
        private readonly Channel<TickData> _channel;
        private readonly ConcurrentDictionary<AggregatorKey, InMemoryCandle> _activeCandles = new();
        private readonly TimeSpan _candleInterval;
        private readonly ITimeService _timeService;
        private readonly ILogger<TickAggregator> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly int _flushIntervalSeconds;
        private readonly KafkaCandleProducer? _kafkaCandleProducer;
        private readonly KafkaOptions _kafkaOptions;
        private readonly bool _useKafka;
        private readonly bool _enabled;

        private Task _processingTask = Task.CompletedTask;
        private Timer _flushTimer = null!;
        private readonly CancellationTokenSource _cts = new();


        /// <summary>
        /// Внутреннее представление свечи в памяти.
        /// Value-type — избегает heap-аллокаций.
        /// Ticker/Exchange/Interval хранятся в ключе словаря или вычисляются.
        /// </summary>
        private struct InMemoryCandle
        {
            public DateTime StartTime;
            public DateTime EndTime;
            public decimal Open;
            public decimal High;
            public decimal Low;
            public decimal Close;
            public decimal Volume;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Update(decimal price, decimal volume)
            {
                if (price > High) High = price;
                if (price < Low) Low = price;
                Close = price;
                Volume += volume;
            }
        }

        public TickAggregator(
            ITimeService timeService,
            ILogger<TickAggregator> logger,
            IServiceScopeFactory scopeFactory,
            IOptions<TickAggregatorOptions> options,
            KafkaCandleProducer? kafkaCandleProducer = null,
            IOptions<KafkaOptions>? kafkaOptions = null)
        {
            _timeService = timeService ?? throw new ArgumentNullException(nameof(timeService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = options.Value ?? throw new ArgumentNullException(nameof(options));
            _flushIntervalSeconds = options.Value.FlushIntervalSeconds;
            _candleInterval = TimeSpan.FromSeconds(options.Value.CandleIntervalSeconds);

            _kafkaCandleProducer = kafkaCandleProducer;
            _kafkaOptions = kafkaOptions?.Value ?? new KafkaOptions();
            _useKafka = _kafkaOptions.Enabled && _kafkaCandleProducer != null;

            _channel = Channel.CreateBounded<TickData>(new BoundedChannelOptions(options.Value.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = false,
                SingleWriter = false
            });

            _enabled = options.Value.Enabled;

            if (!_enabled)
            {
                _logger.LogInformation("TickAggregator отключён (Enabled=false). Тики будут игнорироваться.");
                return;
            }

            if (_useKafka)
            {
                _logger.LogInformation(
                    "TickAggregator will publish candles to Kafka topic={Topic}",
                    _kafkaOptions.AggregatedDataTopic);
            }
            else
            {
                _logger.LogInformation("TickAggregator will write candles directly to database (Kafka disabled)");
            }
        }

        public Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
        {
            if (!_enabled) return Task.CompletedTask;
            return _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange)).AsTask();
        }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (!_enabled)
            {
                _logger.LogInformation("TickAggregator.StartAsync пропущен (Enabled=false).");
                return Task.CompletedTask;
            }

            _processingTask = ProcessChannelAsync(_cts.Token);

            _flushTimer = new Timer(async _ => await FlushCompletedCandlesAsync(), null,
                TimeSpan.FromSeconds(_flushIntervalSeconds),
                TimeSpan.FromSeconds(_flushIntervalSeconds));

            _logger.LogInformation(
                "TickAggregator запущен, интервал свечи {CandleInterval}s, flush каждые {FlushInterval}с",
                (int)_candleInterval.TotalSeconds, _flushIntervalSeconds);
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            if (!_enabled)
            {
                _logger.LogInformation("TickAggregator.StopAsync пропущен (Enabled=false).");
                return;
            }

            _logger.LogInformation("TickAggregator: остановка...");

            _flushTimer?.Dispose();

            // Завершаем запись в канал
            _channel.Writer.TryComplete();

            // Ждём завершения обработки оставшихся тиков.
            // Используем внутренний timeout 15с вместо внешнего cancellationToken,
            // т.к. внешний токен может быть уже отменён (например, при остановке Worker).
            try
            {
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                await _processingTask.WaitAsync(timeoutCts.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("TickAggregator: превышен таймаут ожидания обработки (15с)");
            }

            // Финальный flush всех оставшихся свечей (включая незавершённые)
            await FlushAllCandlesAsync();

            _logger.LogInformation("TickAggregator остановлен");
        }

        /// <summary>
        /// Фоновая задача: читает тики из Channel и обновляет in-memory свечи.
        /// Использует AddOrUpdate вместо GetOrAdd, т.к. struct возвращается по значению.
        /// </summary>
        private async Task ProcessChannelAsync(CancellationToken ct)
        {
            try
            {
                await foreach (var tick in _channel.Reader.ReadAllAsync(ct))
                {
                    var bucketStart = RoundDown(tick.Timestamp, _candleInterval);
                    var key = new AggregatorKey(tick.Ticker, tick.Exchange, bucketStart.Ticks);
                    var endTime = bucketStart + _candleInterval;

                    _activeCandles.AddOrUpdate(
                        key,
                        addValueFactory: key =>
                        {
                            var c = new InMemoryCandle
                            {
                                StartTime = bucketStart,
                                EndTime = endTime,
                                Open = tick.Price,
                                High = tick.Price,
                                Low = tick.Price,
                                Close = tick.Price,
                                Volume = 0m
                            };
                            c.Update(tick.Price, tick.Volume);
                            return c;
                        },
                        updateValueFactory: (key, existing) =>
                        {
                            existing.Update(tick.Price, tick.Volume);
                            return existing;
                        });
                }
            }
            catch (OperationCanceledException)
            {
                // Ожидаемо при остановке
            }
        }

        /// <summary>
        /// Сброс завершённых свечей (EndTime <= Now) в БД.
        /// Вызывается по таймеру.
        /// </summary>
        private async Task FlushCompletedCandlesAsync()
        {
            try
            {
                var now = _timeService.UtcNow;
                var completedPairs = new List<KeyValuePair<AggregatorKey, InMemoryCandle>>();

                foreach (var kvp in _activeCandles)
                {
                    if (kvp.Value.EndTime <= now)
                    {
                        completedPairs.Add(kvp);
                    }
                }

                foreach (var pair in completedPairs)
                {
                    _activeCandles.TryRemove(pair.Key, out _);
                }

                if (completedPairs.Count > 0)
                {
                    await SaveCandlesAsync(completedPairs);
                    _logger.LogDebug("Сброшено {Count} завершённых свечей", completedPairs.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при сбросе завершённых свечей");
            }
        }

        /// <summary>
        /// Финальный сброс всех оставшихся свечей (включая незавершённые) при остановке.
        /// </summary>
        private async Task FlushAllCandlesAsync()
        {
            try
            {
                var pairs = _activeCandles.ToList();
                _activeCandles.Clear();

                if (pairs.Count > 0)
                {
                    await SaveCandlesAsync(pairs);
                    _logger.LogInformation("Финальный flush: сохранено {Count} свечей", pairs.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при финальном flush свечей");
            }
        }

        /// <summary>
        /// Сохранение списка свечей: через Kafka (если включено) или напрямую в БД.
        /// При ошибке Kafka выполняется fallback на прямую запись в БД.
        /// </summary>
        private async Task SaveCandlesAsync(List<KeyValuePair<AggregatorKey, InMemoryCandle>> candles)
        {
            if (_useKafka && _kafkaCandleProducer != null)
            {
                try
                {
                    await SaveCandlesViaKafkaAsync(candles);
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Kafka publish failed for {Count} candles, falling back to direct DB write",
                        candles.Count);
                }
            }

            await SaveCandlesViaDatabaseAsync(candles);
        }

        /// <summary>
        /// Публикация свечей в Kafka topic aggregated-data.
        /// Ticker/Exchange берутся из ключа словаря, Interval вычисляется.
        /// </summary>
        private async Task SaveCandlesViaKafkaAsync(List<KeyValuePair<AggregatorKey, InMemoryCandle>> candles)
        {
            var interval = FormatInterval(_candleInterval);

            foreach (var pair in candles)
            {
                var (key, candle) = pair;
                await _kafkaCandleProducer!.ProduceAsync(
                    key.Ticker,
                    interval,
                    candle.Open,
                    candle.High,
                    candle.Low,
                    candle.Close,
                    candle.Volume,
                    candle.StartTime,
                    candle.EndTime,
                    key.Exchange,
                    CancellationToken.None);
            }

            _logger.LogDebug(
                "Опубликовано {Count} свечей в Kafka topic={Topic}",
                candles.Count, _kafkaOptions.AggregatedDataTopic);
        }

        /// <summary>
        /// Сохранение списка свечей напрямую в БД через репозиторий (fallback).
        /// Ticker/Exchange берутся из ключа словаря, Interval вычисляется.
        /// </summary>
        private async Task SaveCandlesViaDatabaseAsync(List<KeyValuePair<AggregatorKey, InMemoryCandle>> candles)
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();
            var interval = FormatInterval(_candleInterval);

            var entities = candles.Select(pair =>
            {
                var (key, candle) = pair;
                return new AggregatedData(
                    key.Ticker, interval, candle.Open, candle.High,
                    candle.Low, candle.Close, candle.Volume,
                    candle.StartTime, candle.EndTime, _timeService);
            }).ToList();

            await repository.AddRangeAsync(entities);
            await repository.SaveChangesAsync();
        }

        /// <summary>
        /// Округление времени вниз до начала интервала.
        /// Пример: 12:34:17, 1m → 12:34:00
        /// </summary>
        private static DateTime RoundDown(DateTime timestamp, TimeSpan interval)
        {
            var ticks = timestamp.Ticks / interval.Ticks * interval.Ticks;
            return new DateTime(ticks, timestamp.Kind);
        }

        /// <summary>
        /// Форматирование интервала в строку для сохранения в БД.
        /// Примеры: 60s → "60s", 120s → "2m", 30s → "30s".
        /// </summary>
        private static string FormatInterval(TimeSpan interval)
        {
            if (interval.TotalMinutes >= 1 && interval.TotalSeconds % 60 == 0)
                return $"{(int)interval.TotalMinutes}m";
            return $"{(int)interval.TotalSeconds}s";
        }
    }
}