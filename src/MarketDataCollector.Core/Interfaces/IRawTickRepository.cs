using MarketDataCollector.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IRawTickRepository : IRepository<RawTick>
    {
        Task<IEnumerable<RawTick>> GetByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<IEnumerable<RawTick>> GetByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<bool> ExistsAsync(string ticker, string exchange, DateTime timestamp, CancellationToken cancellationToken = default);
        Task<int> GetCountAsync(DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<IEnumerable<RawTick>> GetUnnormalizedAsync(int limit = 1000, CancellationToken cancellationToken = default);
    }
}
