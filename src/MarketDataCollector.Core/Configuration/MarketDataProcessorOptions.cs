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
        /// Интервал принудительного сброса неполных батчей в БД (в секундах).
        /// Если за это время не набрался полный батч (BatchSize),
        /// частичный батч сбрасывается принудительно.
        /// 0 = отключено (только полные батчи).
        /// Значение по умолчанию 0 — включается через конфигурацию (appsettings.json),
        /// где установлено 5 секунд.
        /// </summary>
        public int FlushIntervalSeconds { get; set; } = 0;

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
        /// - Запускается ConsumerCount parallel consumer'ов (если ConsumerCount > 0)
        ///   либо Math.Clamp(CPU/2, 1, 4) при ConsumerCount = 0
        /// - Вставка в БД сериализована через SemaphoreSlim(1,1) в BulkCopyAsync
        ///
        /// По результатам бенчмарка: Sequential batch=700 даёт ~62 680 ticks/sec,
        /// что достаточно для текущей нагрузки на одном потоке.
        /// </summary>
        public bool UseSingleConsumer { get; set; } = false;

        /// <summary>
        /// Количество parallel consumer'ов для режима Multiple Consumers (UseSingleConsumer=false).
        /// 0 = авто-определение (Math.Clamp(CPU/2, 1, 4), по умолчанию).
        /// Значение больше 0 — фиксированное количество consumer'ов.
        /// </summary>
        public int ConsumerCount { get; set; } = 0;
    }
}
