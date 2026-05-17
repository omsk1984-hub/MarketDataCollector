using MarketDataCollector.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IAggregatedDataRepository : IRepository<AggregatedData>
    {
        Task<IEnumerable<AggregatedData>> GetByTickerAndIntervalAsync(
            string ticker, string interval,
            DateTime? from = null, DateTime? to = null,
            CancellationToken cancellationToken = default);
    }
}