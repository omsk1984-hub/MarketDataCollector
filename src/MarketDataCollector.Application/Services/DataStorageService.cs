using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarketDataCollector.Application.Services
{
    public class DataStorageService : IDataStorageService
    {
        private readonly IRawTickRepository _rawTickRepository;
        private readonly ILogger<DataStorageService> _logger;

        public DataStorageService(
            IRawTickRepository rawTickRepository,
            ILogger<DataStorageService> logger)
        {
            _rawTickRepository = rawTickRepository;
            _logger = logger;
        }

        public async Task StoreRawTickAsync(RawTick rawTick)
        {
            try
            {
                await _rawTickRepository.AddAsync(rawTick);
                await _rawTickRepository.SaveChangesAsync();
                
                _logger.LogDebug("Raw tick stored: {Id} {Ticker} {Price}", rawTick.Id, rawTick.Ticker, rawTick.Price);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error storing raw tick: {Ticker} {Price}", rawTick.Ticker, rawTick.Price);
                throw;
            }
        }

        public async Task StoreRawTicksBatchAsync(IEnumerable<RawTick> rawTicks)
        {
            try
            {
                await _rawTickRepository.AddRangeAsync(rawTicks);
                await _rawTickRepository.SaveChangesAsync();
                
                _logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks.Count());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error storing batch of raw ticks");
                throw;
            }
        }

        public async Task<IEnumerable<RawTick>> GetTicksByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null)
        {
            try
            {
                return await _rawTickRepository.GetByTickerAsync(ticker, from, to);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving ticks for ticker: {Ticker}", ticker);
                throw;
            }
        }

        public async Task<IEnumerable<RawTick>> GetTicksByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null)
        {
            try
            {
                return await _rawTickRepository.GetByExchangeAsync(exchange, from, to);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving ticks for exchange: {Exchange}", exchange);
                throw;
            }
        }

        public async Task<int> GetTotalTicksCountAsync(DateTime? from = null, DateTime? to = null)
        {
            try
            {
                return await _rawTickRepository.GetCountAsync(from, to);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting total ticks count");
                throw;
            }
        }

        public async Task<bool> TickExistsAsync(string ticker, string exchange, DateTime timestamp)
        {
            try
            {
                return await _rawTickRepository.ExistsAsync(ticker, exchange, timestamp);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking if tick exists");
                throw;
            }
        }

        public async Task<IEnumerable<RawTick>> GetUnnormalizedTicksAsync(int limit = 1000)
        {
            try
            {
                return await _rawTickRepository.GetUnnormalizedAsync(limit);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving unnormalized ticks");
                throw;
            }
        }

        public async Task MarkAsNormalizedAsync(Guid tickId)
        {
            try
            {
                var tick = await _rawTickRepository.GetByIdAsync(tickId);
                if (tick != null)
                {
                    tick.MarkAsNormalized();
                    _rawTickRepository.Update(tick);
                    await _rawTickRepository.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking tick as normalized: {TickId}", tickId);
                throw;
            }
        }
    }

    public interface IDataStorageService
    {
        Task StoreRawTickAsync(RawTick rawTick);
        Task StoreRawTicksBatchAsync(IEnumerable<RawTick> rawTicks);
        Task<IEnumerable<RawTick>> GetTicksByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null);
        Task<IEnumerable<RawTick>> GetTicksByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null);
        Task<int> GetTotalTicksCountAsync(DateTime? from = null, DateTime? to = null);
        Task<bool> TickExistsAsync(string ticker, string exchange, DateTime timestamp);
        Task<IEnumerable<RawTick>> GetUnnormalizedTicksAsync(int limit = 1000);
        Task MarkAsNormalizedAsync(Guid tickId);
    }
}