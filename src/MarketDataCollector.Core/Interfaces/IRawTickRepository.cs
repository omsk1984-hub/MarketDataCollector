using MarketDataCollector.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IRawTickRepository : IRepository<RawTick>
    {
        Task<IEnumerable<RawTick>> GetByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null);
        Task<IEnumerable<RawTick>> GetByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null);
        Task<bool> ExistsAsync(string ticker, string exchange, DateTime timestamp);
        Task<int> GetCountAsync(DateTime? from = null, DateTime? to = null);
        Task<IEnumerable<RawTick>> GetUnnormalizedAsync(int limit = 1000);
    }
}