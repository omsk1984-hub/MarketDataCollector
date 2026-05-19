using System.ComponentModel.DataAnnotations;

namespace MarketDataCollector.Core.Configuration
{
    public class TickAggregatorOptions
    {
        public const string SectionName = "TickAggregator";

        public const int MinCandleIntervalSeconds = 1;
        public const int MaxCandleIntervalSeconds = 3600; // 1 час

        /// <summary>
        /// Включить агрегацию тиков в свечи. По умолчанию true.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Интервал свечи в секундах. По умолчанию 60 = 1m.
        /// </summary>
        [Range(MinCandleIntervalSeconds, MaxCandleIntervalSeconds)]
        public int CandleIntervalSeconds { get; set; } = 60;

        /// <summary>
        /// Интервал сброса завершённых свечей в БД (в секундах).
        /// </summary>
        public int FlushIntervalSeconds { get; set; } = 5;

        /// <summary>
        /// Ёмкость Channel для буферизации тиков перед агрегацией.
        /// </summary>
        public int ChannelCapacity { get; set; } = 10000;
    }
}