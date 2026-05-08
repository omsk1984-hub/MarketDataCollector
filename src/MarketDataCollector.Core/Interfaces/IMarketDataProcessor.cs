using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IMarketDataProcessor
    {
        Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
        Task<int> GetProcessedCountAsync();
        void StartProcessing();
        Task StopProcessingAsync(CancellationToken cancellationToken = default);
    }
}
