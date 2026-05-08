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
    }
}
