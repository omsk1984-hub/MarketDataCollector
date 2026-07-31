using System;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Domain.Utilities;

namespace MarketDataCollector.Domain.Entities
{
    /// <summary>
    /// Агрегированная OHLCV-свеча. Чистая доменная сущность — без персистентностных атрибутов.
    /// Маппинг в БД выполняется через Fluent API в Infrastructure.
    /// </summary>
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
            OpenPrice = DecimalHelper.TruncateForDatabase(openPrice);
            HighPrice = DecimalHelper.TruncateForDatabase(highPrice);
            LowPrice = DecimalHelper.TruncateForDatabase(lowPrice);
            ClosePrice = DecimalHelper.TruncateForDatabase(closePrice);
            Volume = DecimalHelper.TruncateForDatabase(volume);
            StartTime = startTime;
            EndTime = endTime;
            CreatedAt = timeService?.UtcNow ?? throw new ArgumentNullException(nameof(timeService));
        }

        public void UpdatePrices(decimal high, decimal low, decimal close, decimal volume)
        {
            if (high > HighPrice) HighPrice = DecimalHelper.TruncateForDatabase(high);
            if (low < LowPrice) LowPrice = DecimalHelper.TruncateForDatabase(low);
            ClosePrice = DecimalHelper.TruncateForDatabase(close);
            Volume = DecimalHelper.TruncateForDatabase(Volume + volume);
        }
    }
}
