using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IRawTickRepository : IRepository<RawTick>
    {
        Task<IEnumerable<RawTick>> GetByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<IEnumerable<RawTick>> GetByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<bool> ExistsAsync(string ticker, string exchange, DateTime timestamp, CancellationToken cancellationToken = default);
        Task<HashSet<(string Ticker, string Exchange, DateTime Timestamp)>> ExistsBatchAsync(
            IEnumerable<(string Ticker, string Exchange, DateTime Timestamp)> keys,
            CancellationToken cancellationToken = default);
        Task<int> BulkInsertIgnoreConflictsAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Быстрая массовая вставка сырых тиков.
        /// Использует эффективный механизм (например, массивы-параметры с UNNEST)
        /// и игнорирует конфликты уникальности (ON CONFLICT DO NOTHING).
        /// Возвращает количество вставленных строк.
        /// По производительности в 10-100x быстрее BulkInsertIgnoreConflictsAsync.
        /// </summary>
        Task<int> BulkInsertFastAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default);

        /// <summary>
        /// Быстрая массовая вставка напрямую из TickData (без промежуточного создания List<RawTick>).
        /// Создаёт RawTick внутри в одном проходе — устраняет двойную итерацию.
        /// </summary>
        Task<int> BulkInsertFastAsync(IReadOnlyList<TickData> ticks, ITimeService timeService, CancellationToken cancellationToken = default);
        Task<int> GetCountAsync(DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default);
        Task<IEnumerable<RawTick>> GetUnnormalizedAsync(int limit = 1000, CancellationToken cancellationToken = default);
    }
}
