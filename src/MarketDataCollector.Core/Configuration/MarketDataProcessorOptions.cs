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

        /// <summary>
        /// Режим Single Consumer: использует ровно 1 consumer вместо N параллельных.
        ///
        /// Когда true:
        /// - Channel создаётся с SingleReader=true (гарантия однопоточного чтения)
        /// - Запускается ровно 1 consumer, который последовательно читает тики и пишет батчи
        /// - Полностью исключает deadlock'и (40P01) за счёт отсутствия конкуренции потоков
        ///
        /// Когда false (по умолчанию):
        /// - Channel с SingleReader=false
        /// - Запускается Math.Clamp(CPU/2, 1, 4) parallel consumer'ов
        /// - Вставка в БД сериализована через SemaphoreSlim(1,1) в BulkCopyAsync
        ///
        /// По результатам бенчмарка: Sequential batch=700 даёт ~62 680 ticks/sec,
        /// что достаточно для текущей нагрузки на одном потоке.
        /// </summary>
        public bool UseSingleConsumer { get; set; } = false;
    }
}
