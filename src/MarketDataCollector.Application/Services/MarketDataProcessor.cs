using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MarketDataCollector.Application.Services
{
    public class MarketDataProcessor : IMarketDataProcessor
    {
        private readonly IRawTickRepository _rawTickRepository;
        private readonly ILogger<MarketDataProcessor> _logger;
        private readonly ITimeService _timeService;
        private readonly Channel<TickData> _channel;
        public Channel<TickData> Channel => _channel;
        private readonly int _batchSize;

        private Task _processingTask = null!;
        private int _processedCount;

        public event EventHandler<Exception>? OnError;

        public readonly record struct TickData(
            string Ticker,
            decimal Price,
            decimal Volume,
            DateTime Timestamp,
            string Exchange
        );

        public MarketDataProcessor(
            IRawTickRepository rawTickRepository,
            ILogger<MarketDataProcessor> logger,
            ITimeService timeService,
            int batchSize,
            int channelCapacity)
        {
            _rawTickRepository = rawTickRepository;
            _logger = logger;
            _timeService = timeService;
            _batchSize = batchSize;
            _processedCount = 0;

            _channel = System.Threading.Channels.Channel.CreateBounded<TickData>(new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            });
        }

        public async Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
        {
            await _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange));
            _logger.LogDebug("Тик добавлен в очередь: {Ticker} {Price} {Volume} {Exchange}", ticker, price, volume, exchange);
        }

        public Task StartProcessingAsync(CancellationToken cancellationToken = default)
        {
            if (_processingTask != null && !_processingTask.IsCompleted)
                return Task.CompletedTask;

            // Логируем ошибку предыдущей задачи, если она завершилась с ошибкой
            if (_processingTask?.IsFaulted == true)
            {
                _logger.LogError(_processingTask.Exception?.InnerException ?? _processingTask.Exception,
                    "Предыдущая задача обработки завершилась ошибкой, перезапуск");
            }

            // Запускаем обработку напрямую без избыточного Task.Run
            _processingTask = ProcessBatchesAsync(cancellationToken);
            _logger.LogInformation("Обработчик рыночных данных запущен batchSize={_batchSize}", _batchSize);
            
            return _processingTask;
        }

        public async Task StopProcessingAsync(CancellationToken cancellationToken = default)
        {
            _channel.Writer.Complete();
            
            if (_processingTask != null)
            {
                try
                {
                    await _processingTask.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogWarning("Остановка обработки отменена");
                }
            }
            
            _logger.LogInformation("Обработчик рыночных данных остановлен. Всего обработано: {Count}", _processedCount);
        }

        private async Task ProcessBatchesAsync(CancellationToken cancellationToken)
        {
            var batch = new List<TickData>(_batchSize);

            try
            {
                await foreach (var tick in _channel.Reader.ReadAllAsync(cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    batch.Add(tick);

                    if (batch.Count >= _batchSize)
                    {
                        await ProcessBatchAsync(batch, cancellationToken);
                        batch.Clear();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Обработка отменена");
            }
            catch (ChannelClosedException)
            {
                // Ожидаемо при завершении канала
            }
            finally
            {
                // Финальный flush
                if (batch.Count > 0)
                {
                    await ProcessBatchAsync(batch, cancellationToken);
                }
            }
        }

        private async Task ProcessBatchAsync(List<TickData> batch, CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                // 1. Убираем дубликаты в памяти
                var uniqueTicks = batch
                    .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))
                    .Select(g => g.First())
                    .ToList();

                // 2. Проверяем существующие в БД одним запросом
                var existingKeys = await GetExistingKeysFromDbAsync(uniqueTicks, cancellationToken);
                var newTicks = uniqueTicks
                    .Where(t => !existingKeys.Contains((t.Ticker, t.Exchange, t.Timestamp)))
                    .ToList();

                if (newTicks.Count == 0)
                {
                    _logger.LogDebug("Все {Count} тиков в батче были дубликатами", batch.Count);
                    return;
                }

                // 3. Bulk insert
                var entities = newTicks.Select(t => new RawTick(
                    t.Ticker, t.Price, t.Volume, t.Timestamp, t.Exchange, _timeService
                )).ToList();

                await _rawTickRepository.AddRangeAsync(entities, cancellationToken);
                await _rawTickRepository.SaveChangesAsync(cancellationToken);

                var count = Interlocked.Add(ref _processedCount, entities.Count);
                
                if (count % 100 < entities.Count)
                {
                    _logger.LogInformation("Всего обработано тиков: {Count}", count);
                }

                _logger.LogDebug("Батч сохранён: {Saved} новых, {Duplicates} дубликатов пропущено",
                    entities.Count, batch.Count - entities.Count);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Обработка батча отменена");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Критическая ошибка при обработке батча из {Count} тиков", batch.Count);
                OnError?.Invoke(this, ex);
            }
        }

        private async Task<HashSet<(string, string, DateTime)>> GetExistingKeysFromDbAsync(
            List<TickData> ticks,
            CancellationToken cancellationToken)
        {
            // Для простоты проверяем каждый тик отдельно
            // В продакшене лучше использовать один запрос с WHERE IN
            var existing = new HashSet<(string, string, DateTime)>();
            
            foreach (var tick in ticks)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (await _rawTickRepository.ExistsAsync(tick.Ticker, tick.Exchange, tick.Timestamp, cancellationToken))
                {
                    existing.Add((tick.Ticker, tick.Exchange, tick.Timestamp));
                }
            }
            
            return existing;
        }

        public Task<int> GetProcessedCountAsync()
        {
            return Task.FromResult(_processedCount);
        }
    }
}
