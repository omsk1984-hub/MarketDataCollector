namespace MarketDataCollector.Core.Configuration
{
    public class MarketDataProcessorOptions
    {
        public const string SectionName = "MarketDataProcessor";
        
        public int BatchSize { get; set; } = 100;
        public int BatchTimeoutMs { get; set; } = 1000;
        public int ChannelCapacity { get; set; } = 10000;
    }
}
