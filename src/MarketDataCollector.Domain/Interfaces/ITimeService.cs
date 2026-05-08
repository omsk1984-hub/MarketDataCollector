using System;

namespace MarketDataCollector.Domain.Interfaces
{
    public interface ITimeService
    {
        DateTime UtcNow { get; }
    }
}