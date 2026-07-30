namespace MarketDataCollector.Core.Configuration
{
    public class MarketDataProcessorOptions
    {
        public const string SectionName = "MarketDataProcessor";
        
        /// <summary>
        /// Размер батча для записи в БД через Binary COPY protocol (legacy).
        /// Используется как значение по умолчанию для MaxBatchSize, если MaxBatchSize = 0.
        /// По результатам бенчмарка: chunk=800 даёт ~53 775 ticks/sec
        /// при 8 parallel consumer'ах через BulkCopyAsync.
        /// </summary>
        public int BatchSize { get; set; } = 5000;

        /// <summary>
        /// Минимальный размер батча при адаптивном режиме.
        /// При низком backlog Collector использует MinBatchSize — снижает GC pressure.
        /// </summary>
        public int MinBatchSize { get; set; } = 1000;

        /// <summary>
        /// Максимальный размер батча при адаптивном режиме.
        /// При высоком backlog Collector увеличивает BatchSize до MaxBatchSize.
        /// Если 0 — используется BatchSize как MaxBatchSize.
        /// </summary>
        public int MaxBatchSize { get; set; } = 0;

        /// <summary>
        /// Ёмкость bounded-канала System.Threading.Channels.Channel<TickData>
        /// для буферизации входящих тиков перед обработкой.
        /// При превышении лимита новые тики вытесняют старые (DropOldest),
        /// что защищает потребителя от перегрузки при всплесках.
        /// 150000 — эмпирическое значение для ~20K msg/sec с учётом backlog.
        /// </summary>
        public int ChannelCapacity { get; set; } = 150000;

        /// <summary>
        /// Ёмкость bounded-канала между Collector и Writer (Channel<CollectedBatch>).
        /// Каждый элемент — один batch (до MaxBatchSize тиков).
        /// При capacity=20 максимальный буфер = 20 × 2500 = 50 000 тиков.
        /// FullMode=Wait: при переполнении Collector блокируется → backpressure
        /// на input channel → DropOldest на входе.
        /// </summary>
        public int BatchChannelCapacity { get; set; } = 20;

        /// <summary>
        /// Порог backlog (количество тиков в input channel), при котором
        /// Collector использует MinBatchSize.
        /// </summary>
        public int BacklogLowThreshold { get; set; } = 2000;

        /// <summary>
        /// Порог backlog, при котором Collector использует MaxBatchSize.
        /// Между low и high — линейная интерполяция.
        /// </summary>
        public int BacklogHighThreshold { get; set; } = 5000;

        /// <summary>
        /// Если последняя запись батча заняла больше этого значения (ms),
        /// BatchSize временно снижается на 20% для предотвращения каскадного роста backlog.
        /// 0 = отключено.
        /// </summary>
        public double WriteDurationWarningMs { get; set; } = 200.0;

        /// <summary>
        /// Интервал принудительного сброса неполных батчей в БД (в секундах).
        /// Если за это время не набрался полный батч (MinBatchSize),
        /// частичный батч сбрасывается принудительно.
        /// 0 = отключено (только полные батчи).
        /// </summary>
        public int FlushIntervalSeconds { get; set; } = 0;

        /// <summary>
        /// Режим Single Consumer: использует ровно 1 consumer вместо N параллельных.
        ///
        /// Когда true (рекомендовано):
        /// - Channel создаётся с SingleReader=true (гарантия однопоточного чтения)
        /// - Запускается ровно 1 Collector + 1 Writer
        /// - Полностью исключает deadlock'и (40P01) за счёт отсутствия конкуренции потоков
        /// - Меньше GC-давления и lock contention
        ///
        /// Когда false:
        /// - Per-ticker routing (хэш от ticker'а) гарантирует disjoint наборы тикеров
        /// - Consumer'ы работают с разными тикерами — deadlock'и (40P01) невозможны
        /// - Запускается ConsumerCount parallel consumer'ов (если ConsumerCount > 0)
        ///   либо Math.Clamp(CPU/2, 1, 4) при ConsumerCount = 0
        /// - Parallelism полезен при throughput > 25K ticks/sec
        ///
        /// По результатам бенчмарка: Sequential batch=2500 даёт ~10,700 ticks/sec,
        /// что достаточно для текущей нагрузки (~19K msg/s).
        /// </summary>
        public bool UseSingleConsumer { get; set; } = true;

        /// <summary>
        /// Количество parallel consumer'ов для режима Multiple Consumers (UseSingleConsumer=false).
        /// 0 = авто-определение (Math.Clamp(CPU/2, 1, 4), по умолчанию).
        /// Значение больше 0 — фиксированное количество consumer'ов.
        /// </summary>
        public int ConsumerCount { get; set; } = 0;

        /// <summary>
        /// Максимальный размер кэша дедупликации (количество записей).
        /// Тики с ключом (ticker, exchange, timestamp), попавшие в кэш,
        /// пропускаются перед BulkCopyAsync — экономят INSERT.
        /// FIFO-эвикция: при превышении лимита самая старая запись удаляется.
        /// 0 = кэш отключён.
        /// 50000 ≈ 5 секунд при 10K ticks/sec — покрывает cross-batch дубли.
        /// </summary>
        public int DeduplicationCacheMaxSize { get; set; } = 50000;
    }
}
