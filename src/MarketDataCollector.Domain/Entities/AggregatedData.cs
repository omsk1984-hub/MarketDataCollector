using System;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    public class AggregatedData
    {
        public Guid Id { get; private set; }
        public string Ticker { get; private set; } = null!;
        public string Interval { get; private set; } = null!;
        public decimal OpenPrice { get; private set; }
        public decimal HighPrice { get; private set; }
        public decimal LowPrice { get; private set; }
        public decimal ClosePrice { get; private set; }
        public decimal Volume { get; private set; }
        public DateTime StartTime { get; private set; }
        public DateTime EndTime { get; private set; }
        public DateTime CreatedAt { get; private set; }

        private AggregatedData() { } // For EF Core

        public AggregatedData(
            string ticker,
            string interval,
            decimal openPrice,
            decimal highPrice,
            decimal lowPrice,
            decimal closePrice,
            decimal volume,
            DateTime startTime,
            DateTime endTime,
            ITimeService timeService)
        {
            Id = Guid.NewGuid();
            Ticker = ticker ?? throw new ArgumentNullException(nameof(ticker));
            Interval = interval ?? throw new ArgumentNullException(nameof(interval));
            OpenPrice = openPrice;
            HighPrice = highPrice;
            LowPrice = lowPrice;
            ClosePrice = closePrice;
            Volume = volume;
            StartTime = startTime;
            EndTime = endTime;
            CreatedAt = timeService?.UtcNow ?? throw new ArgumentNullException(nameof(timeService));
        }

        public void UpdatePrices(decimal high, decimal low, decimal close, decimal volume)
        {
            if (high > HighPrice) HighPrice = high;
            if (low < LowPrice) LowPrice = low;
            ClosePrice = close;
            Volume += volume;
        }
    }
}