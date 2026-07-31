using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    /// <summary>
    /// Абстракция публикации OHLCV-свечей во внешний транспорт (Kafka и т.п.).
    /// Application-слой зависит только от этого интерфейса, а не от конкретной
    /// инфраструктурной реализации.
    /// </summary>
    public interface ICandlePublisher
    {
        /// <summary>
        /// Опубликовать свечу во внешний транспорт.
        /// </summary>
        Task ProduceAsync(
            string ticker,
            string interval,
            decimal open,
            decimal high,
            decimal low,
            decimal close,
            decimal volume,
            DateTime startTime,
            DateTime endTime,
            string exchange,
            CancellationToken cancellationToken = default);
    }
}
