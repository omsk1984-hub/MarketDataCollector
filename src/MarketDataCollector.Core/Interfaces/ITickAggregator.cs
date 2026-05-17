using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    /// <summary>
    /// Агрегатор тиковых данных в OHLCV-свечи (1m).
    /// </summary>
    public interface ITickAggregator
    {
        /// <summary>
        /// Передать тик в агрегатор.
        /// </summary>
        Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);

        /// <summary>
        /// Запустить фоновую обработку (чтение канала, таймер flush'а).
        /// </summary>
        Task StartAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Остановить обработку и выполнить финальный flush.
        /// </summary>
        Task StopAsync(CancellationToken cancellationToken = default);
    }
}