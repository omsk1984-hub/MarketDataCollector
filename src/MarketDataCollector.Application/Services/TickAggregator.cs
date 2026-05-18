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
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MarketDataCollector.Application.Services
{
    /// <summary>
    /// Агрегатор тиковых данных в OHLCV-свечи заданного интервала.
    /// Работает независимо от основного пайплайна записи RawTicks.
    /// </summary>
    public class TickAggregator : ITickAggregator
    {
        private readonly Channel<TickData> _channel;
        private readonly ConcurrentDictionary<string, InMemoryCandle> _activeCandles = new();
        private readonly TimeSpan _candleInterval;
        private readonly ITimeService _timeService;
        private readonly ILogger<TickAggregator> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly int _flushIntervalSeconds;
        private readonly KafkaCandleProducer? _kafkaCandleProducer;
        private readonly KafkaOptions _kafkaOptions;
        private readonly bool _useKafka;

        private Task _processingTask = Task.CompletedTask;
        private Timer _flushTimer = null!;
        private readonly CancellationTokenSource _cts = new();

        public readonly record struct TickData(
            string Ticker,
            decimal Price,
            decimal Volume,
            DateTime Timestamp,
            string Exchange
        );

        /// <summary>
        /// Внутреннее представление свечи в памяти.
        /// </summary>
        private class InMemoryCandle
        {
            public string Ticker = null!;
            public string Exchange = null!;
            public string Interval = null!;
            public DateTime StartTime;
            public DateTime EndTime;
            public decimal Open;
            public decimal High;
            public decimal Low;
            public decimal Close;
            public decimal Volume;

            public void Update(decimal price, decimal volume)
            {
                if (price > High) High = price;
                if (price < Low) Low = price;
                Close = price;
                Volume += volume;
            }

            public AggregatedData ToAggregatedData(ITimeService timeService)
            {
                return new AggregatedData(
                    Ticker, Interval, Open, High, Low, Close, Volume,
                    StartTime, EndTime, timeService);
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
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });

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
            return _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange)).AsTask();
        }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
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
            _logger.LogInformation("TickAggregator: остановка...");

            _flushTimer?.Dispose();

            // Завершаем запись в канал
            _channel.Writer.TryComplete();

            // Ждём завершения обработки оставшихся тиков
            try
            {
                await _processingTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("TickAggregator: ожидание обработки отменено");
            }

            // Финальный flush всех оставшихся свечей (включая незавершённые)
            await FlushAllCandlesAsync();

            _logger.LogInformation("TickAggregator остановлен");
        }

        /// <summary>
        /// Фоновая задача: читает тики из Channel и обновляет in-memory свечи.
        /// </summary>
        private async Task ProcessChannelAsync(CancellationToken ct)
        {
            try
            {
                await foreach (var tick in _channel.Reader.ReadAllAsync(ct))
                {
                    var bucketStart = RoundDown(tick.Timestamp, _candleInterval);
                    var key = $"{tick.Ticker}|{tick.Exchange}|{bucketStart:O}";

                    var candle = _activeCandles.GetOrAdd(key, _ => new InMemoryCandle
                    {
                        Ticker = tick.Ticker,
                        Exchange = tick.Exchange,
                        Interval = FormatInterval(_candleInterval),
                        StartTime = bucketStart,
                        EndTime = bucketStart + _candleInterval,
                        Open = tick.Price,
                        High = tick.Price,
                        Low = tick.Price,
                        Close = tick.Price,
                        Volume = 0m
                    });

                    candle.Update(tick.Price, tick.Volume);
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
                var completedKeys = new List<string>();
                var completedCandles = new List<InMemoryCandle>();

                foreach (var kvp in _activeCandles)
                {
                    if (kvp.Value.EndTime <= now)
                    {
                        completedKeys.Add(kvp.Key);
                        completedCandles.Add(kvp.Value);
                    }
                }

                foreach (var key in completedKeys)
                {
                    _activeCandles.TryRemove(key, out _);
                }

                if (completedCandles.Count > 0)
                {
                    await SaveCandlesAsync(completedCandles);
                    _logger.LogDebug("Сброшено {Count} завершённых свечей", completedCandles.Count);
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
                var candles = _activeCandles.Values.ToList();
                _activeCandles.Clear();

                if (candles.Count > 0)
                {
                    await SaveCandlesAsync(candles);
                    _logger.LogInformation("Финальный flush: сохранено {Count} свечей", candles.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при финальном flush свечей");
            }
        }

        /// <summary>
        /// Сохранение списка свечей: через Kafka (если включено) или напрямую в БД.
        /// </summary>
        private async Task SaveCandlesAsync(List<InMemoryCandle> candles)
        {
            if (_useKafka && _kafkaCandleProducer != null)
            {
                await SaveCandlesViaKafkaAsync(candles);
            }
            else
            {
                await SaveCandlesViaDatabaseAsync(candles);
            }
        }

        /// <summary>
        /// Публикация свечей в Kafka topic aggregated-data.
        /// </summary>
        private async Task SaveCandlesViaKafkaAsync(List<InMemoryCandle> candles)
        {
            foreach (var candle in candles)
            {
                await _kafkaCandleProducer!.ProduceAsync(
                    candle.Ticker,
                    candle.Interval,
                    candle.Open,
                    candle.High,
                    candle.Low,
                    candle.Close,
                    candle.Volume,
                    candle.StartTime,
                    candle.EndTime,
                    candle.Exchange,
                    CancellationToken.None);
            }

            _logger.LogDebug(
                "Опубликовано {Count} свечей в Kafka topic={Topic}",
                candles.Count, _kafkaOptions.AggregatedDataTopic);
        }

        /// <summary>
        /// Сохранение списка свечей напрямую в БД через репозиторий (fallback).
        /// </summary>
        private async Task SaveCandlesViaDatabaseAsync(List<InMemoryCandle> candles)
        {
            using var scope = _scopeFactory.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();

            var entities = candles.Select(c => c.ToAggregatedData(_timeService)).ToList();
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