namespace MarketDataCollector.Core.Configuration
{
    public class MarketDataProcessorOptions
    {
        public const string SectionName = "MarketDataProcessor";
        
        /// <summary>
        /// Размер батча для записи в БД через Binary COPY protocol.
        /// По результатам бенчмарка: chunk=800 даёт ~53 775 ticks/sec
        /// при 8 parallel consumer'ах через BulkCopyAsync.
        /// </summary>
        public int BatchSize { get; set; } = 800;
        public int ChannelCapacity { get; set; } = 10000;
    }
}
