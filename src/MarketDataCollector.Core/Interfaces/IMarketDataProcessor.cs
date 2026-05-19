using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IMarketDataProcessor
    {
        /// <summary>
        /// Вызывается при критической ошибке обработки данных.
        /// Подписчик должен инициировать остановку Worker.
        /// </summary>
        event EventHandler<Exception>? OnError;

        Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
        Task<int> GetProcessedCountAsync();
        Task StartProcessingAsync(CancellationToken cancellationToken = default);
        Task StopProcessingAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Среднее количество обработанных тиков в секунду за последние 10 секунд.
        /// </summary>
        double GetProcessedRps();

        /// <summary>
        /// Общее количество тиков, поступивших в ProcessTickAsync (до Channel DropOldest).
        /// </summary>
        int GetTotalIncomingCount();

        /// <summary>
        /// Общее количество тиков, успешно прочитанных из канала (после дедупликации на вставку).
        /// </summary>
        int GetTotalReceivedCount();

        /// <summary>
        /// Текущее количество тиков в очереди канала (для мониторинга заполненности).
        /// </summary>
        int GetChannelCount();

        /// <summary>
        /// Ёмкость канала (ChannelCapacity из конфигурации).
        /// Используется совместно с <see cref="GetChannelCount"/> для расчёта процента заполненности.
        /// </summary>
        /// <summary>
        /// Ёмкость канала (ChannelCapacity из конфигурации).
        /// Используется совместно с <see cref="GetChannelCount"/> для расчёта процента заполненности.
        /// </summary>
        int GetChannelCapacity();

        /// <summary>
        /// Количество тиков, реально дропнутых каналом из-за переполнения
        /// (BoundedChannelFullMode.DropOldest).
        /// Сбрасывается при пересоздании канала в StartProcessingAsync.
        /// </summary>
        int GetTotalDroppedCount();
    }
}
