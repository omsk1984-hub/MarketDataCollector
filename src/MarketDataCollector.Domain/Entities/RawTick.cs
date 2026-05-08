using System;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    public class RawTick
    {
        public Guid Id { get; private set; }
        public string Ticker { get; private set; } = null!;
        public decimal Price { get; private set; }
        public decimal Volume { get; private set; }
        public DateTime Timestamp { get; private set; }
        public string Exchange { get; private set; } = null!;
        public DateTime ReceivedAt { get; private set; }
        public bool Normalized { get; private set; }

        private RawTick() { } // For EF Core

        public RawTick(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange, ITimeService timeService)
        {
            Id = Guid.NewGuid();
            Ticker = ticker ?? throw new ArgumentNullException(nameof(ticker));
            Price = price;
            Volume = volume;
            Timestamp = timestamp;
            Exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));
            ReceivedAt = timeService?.UtcNow ?? throw new ArgumentNullException(nameof(timeService));
            Normalized = false;
        }

        public void MarkAsNormalized()
        {
            Normalized = true;
        }

        public void UpdatePrice(decimal newPrice)
        {
            Price = newPrice;
        }

        public void UpdateVolume(decimal newVolume)
        {
            Volume = newVolume;
        }
    }
}