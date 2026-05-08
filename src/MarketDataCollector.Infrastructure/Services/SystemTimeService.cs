using System;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Infrastructure.Services
{
    public class SystemTimeService : ITimeService
    {
        public DateTime UtcNow => DateTime.UtcNow;
    }
}