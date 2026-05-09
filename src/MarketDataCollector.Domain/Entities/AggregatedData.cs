using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    [Table("aggregateddata")]
    public class AggregatedData
    {
        [Key]
        [Column("id")]
        public Guid Id { get; private set; }

        [Column("ticker")]
        [Required]
        [MaxLength(20)]
        public string Ticker { get; private set; } = null!;

        [Column("interval")]
        [Required]
        [MaxLength(10)]
        public string Interval { get; private set; } = null!;

        [Column("openprice")]
        [Required]
        public decimal OpenPrice { get; private set; }

        [Column("highprice")]
        [Required]
        public decimal HighPrice { get; private set; }

        [Column("lowprice")]
        [Required]
        public decimal LowPrice { get; private set; }

        [Column("closeprice")]
        [Required]
        public decimal ClosePrice { get; private set; }

        [Column("volume")]
        [Required]
        public decimal Volume { get; private set; }

        [Column("starttime")]
        [Required]
        public DateTime StartTime { get; private set; }

        [Column("endtime")]
        [Required]
        public DateTime EndTime { get; private set; }

        [Column("createdat")]
        [Required]
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
