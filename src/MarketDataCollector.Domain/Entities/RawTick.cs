using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    [Table("rawticks")]
    public class RawTick
    {
        [Key]
        [Column("id")]
        public Guid Id { get; private set; }

        [Column("ticker")]
        [Required]
        [MaxLength(20)]
        public string Ticker { get; private set; } = null!;

        [Column("price")]
        [Required]
        public decimal Price { get; private set; }

        [Column("volume")]
        [Required]
        public decimal Volume { get; private set; }

        [Column("timestamp")]
        [Required]
        public DateTime Timestamp { get; private set; }

        [Column("exchange")]
        [Required]
        [MaxLength(50)]
        public string Exchange { get; private set; } = null!;

        [Column("receivedat")]
        [Required]
        public DateTime ReceivedAt { get; private set; }

        [Column("normalized")]
        [Required]
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
