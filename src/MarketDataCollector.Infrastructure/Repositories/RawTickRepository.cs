using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Infrastructure.Repositories
{
    public class RawTickRepository : IRawTickRepository
    {
        private readonly MarketDataDbContext _context;
        private readonly DbSet<RawTick> _dbSet;

        public RawTickRepository(MarketDataDbContext context)
        {
            _context = context;
            _dbSet = context.Set<RawTick>();
        }

        public async Task<RawTick?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
        {
            return await _dbSet.FindAsync(new object[] { id }, cancellationToken);
        }

        public async Task<IEnumerable<RawTick>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            return await _dbSet.ToListAsync(cancellationToken);
        }

        public async Task<IEnumerable<RawTick>> FindAsync(Expression<Func<RawTick, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return await _dbSet.Where(predicate).ToListAsync(cancellationToken);
        }

        public async Task AddAsync(RawTick entity, CancellationToken cancellationToken = default)
        {
            await _dbSet.AddAsync(entity, cancellationToken);
        }

        public async Task AddRangeAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default)
        {
            await _dbSet.AddRangeAsync(entities, cancellationToken);
        }

        public void Update(RawTick entity)
        {
            _dbSet.Update(entity);
        }

        public void Remove(RawTick entity)
        {
            _dbSet.Remove(entity);
        }

        public async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            return await _context.SaveChangesAsync(cancellationToken);
        }

        public async Task<IEnumerable<RawTick>> GetByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default)
        {
            var query = _dbSet.Where(t => t.Ticker == ticker);

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.OrderBy(t => t.Timestamp).ToListAsync(cancellationToken);
        }

        public async Task<IEnumerable<RawTick>> GetByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default)
        {
            var query = _dbSet.Where(t => t.Exchange == exchange);

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.OrderBy(t => t.Timestamp).ToListAsync(cancellationToken);
        }

        public async Task<bool> ExistsAsync(string ticker, string exchange, DateTime timestamp, CancellationToken cancellationToken = default)
        {
            return await _dbSet.AnyAsync(t =>
                t.Ticker == ticker &&
                t.Exchange == exchange &&
                t.Timestamp == timestamp, cancellationToken);
        }

        public async Task<HashSet<(string Ticker, string Exchange, DateTime Timestamp)>> ExistsBatchAsync(
            IEnumerable<(string Ticker, string Exchange, DateTime Timestamp)> keys,
            CancellationToken cancellationToken = default)
        {
            var keyList = keys.ToList();
            if (keyList.Count == 0)
                return new HashSet<(string, string, DateTime)>();

            // Формируем параметры для массового WHERE IN запроса
            var tickers = keyList.Select(k => k.Ticker).ToArray();
            var exchanges = keyList.Select(k => k.Exchange).ToArray();
            var timestamps = keyList.Select(k => k.Timestamp).ToArray();

            var existing = await _dbSet
                .Where(t => tickers.Contains(t.Ticker)
                         && exchanges.Contains(t.Exchange)
                         && timestamps.Contains(t.Timestamp))
                .Select(t => new { t.Ticker, t.Exchange, t.Timestamp })
                .ToListAsync(cancellationToken);

            return existing
                .Select(t => (t.Ticker, t.Exchange, t.Timestamp))
                .ToHashSet();
        }

        /// <summary>
        /// Количество повторов при deadlock (PostgreSQL error 40P01).
        /// Deadlock — транзиентная ошибка, повторная попытка обычно успешна.
        /// </summary>
        private const int DeadlockMaxRetries = 5;

        /// <summary>
        /// Базовая задержка между retry при deadlock (экспоненциальная: 200ms, 400ms, 800ms, 1600ms, 3200ms).
        /// </summary>
        private static readonly TimeSpan DeadlockBaseDelay = TimeSpan.FromMilliseconds(200);

        /// <summary>
        /// Максимальный jitter (случайная прибавка к задержке), чтобы избежать
        /// thundering herd при одновременном retry нескольких consumer'ов.
        /// </summary>
        private static readonly TimeSpan DeadlockMaxJitter = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// SemaphoreSlim БОЛЬШЕ НЕ НУЖЕН.
        /// В multiple consumers mode каждый consumer получает disjoint набор тикеров
        /// (per-ticker routing в MarketDataProcessor.ProcessTickAsync через hash ticker'а).
        /// B-tree страницы unique-индекса (ticker, exchange, timestamp) не пересекаются,
        /// поэтому deadlock'и (40P01) невозможны.
        ///
        /// Retry-логика (5 попыток) остаётся safety net'ом на случай других транзиентных
        /// ошибок (timeout, serialization failures, редкие page-level блокировки).
        /// </summary>

        /// <summary>
        /// Источник случайных чисел для jitter. Shared между всеми экземплярами,
        /// т.к. Random не thread-safe — используем ThreadLocal.
        /// </summary>
        private static readonly ThreadLocal<Random> JitterRandom = new(() => new Random());

        [Obsolete("Use BulkCopyAsync (UNNEST-based) instead. This method uses per-row VALUES and is ~10-50x slower.")]
        public async Task<int> BulkInsertIgnoreConflictsAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default)
        {
            var list = entities.ToList();
            if (list.Count == 0)
                return 0;

            // Массовая вставка через raw SQL с ON CONFLICT DO NOTHING
            // Используем UNIQUE constraint на (ticker, exchange, timestamp)
            const string sql = @"
                INSERT INTO rawticks (""id"", ""ticker"", ""price"", ""volume"", ""timestamp"", ""exchange"", ""receivedat"", ""normalized"")
                VALUES {0}
                ON CONFLICT (""ticker"", ""exchange"", ""timestamp"") DO NOTHING;";

            var parameters = new List<Npgsql.NpgsqlParameter>();
            var valueRows = new List<string>();

            for (int i = 0; i < list.Count; i++)
            {
                var entity = list[i];
                parameters.AddRange(new[]
                {
                    new Npgsql.NpgsqlParameter($"@p{i}_id", NpgsqlTypes.NpgsqlDbType.Uuid) { Value = entity.Id },
                    new Npgsql.NpgsqlParameter($"@p{i}_ticker", NpgsqlTypes.NpgsqlDbType.Varchar, 20) { Value = entity.Ticker },
                    new Npgsql.NpgsqlParameter($"@p{i}_price", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = entity.Price },
                    new Npgsql.NpgsqlParameter($"@p{i}_volume", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = entity.Volume },
                    new Npgsql.NpgsqlParameter($"@p{i}_timestamp", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = entity.Timestamp },
                    new Npgsql.NpgsqlParameter($"@p{i}_exchange", NpgsqlTypes.NpgsqlDbType.Varchar, 50) { Value = entity.Exchange },
                    new Npgsql.NpgsqlParameter($"@p{i}_receivedat", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = entity.ReceivedAt },
                    new Npgsql.NpgsqlParameter($"@p{i}_normalized", NpgsqlTypes.NpgsqlDbType.Boolean) { Value = entity.Normalized }
                });

                valueRows.Add($"(@p{i}_id, @p{i}_ticker, @p{i}_price, @p{i}_volume, @p{i}_timestamp, @p{i}_exchange, @p{i}_receivedat, @p{i}_normalized)");
            }

            var formattedSql = string.Format(sql, string.Join(", ", valueRows));

            // Retry loop для транзиентных deadlock'ов (40P01)
            int attempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _context.Database.ExecuteSqlRawAsync(formattedSql, parameters, cancellationToken);
                }
                catch (PostgresException ex) when (ex.SqlState == "40P01" && attempt < DeadlockMaxRetries)
                {
                    attempt++;
                    var delay = DeadlockBaseDelay * (int)Math.Pow(2, attempt - 1);
                    // Jitter для предотвращения thundering herd
                    var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next((int)DeadlockMaxJitter.TotalMilliseconds));
                    await Task.Delay(delay + jitter, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Bulk insert через UNNEST с массивами параметров Npgsql.
        /// Заменяет старый подход с DROP+CREATE temp table + Binary COPY + INSERT.
        ///
        /// Использует один SQL-запрос:
        ///   INSERT INTO rawticks (...)
        ///   SELECT unnest(@arr1), unnest(@arr2), ...
        ///   ON CONFLICT (ticker, exchange, timestamp) DO NOTHING;
        ///
        /// Преимущества:
        /// - Нет DDL (DROP/CREATE) — нет автовакуума, нет overhead
        /// - Один round-trip вместо трёх (DROP, COPY, INSERT)
        /// - Npgsql передаёт массивы как бинарные параметры — ~10-50x быстрее temp table
        ///
        /// Retry: 1 попытка при deadlock (40P01) как safety-net.
        /// Deadlock'и невозможны (per-ticker routing), retry — на случай
        /// других транзиентных ошибок Npgsql.
        /// </summary>
        public async Task<int> BulkCopyAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default)
        {
            var list = entities.ToList();
            if (list.Count == 0)
                return 0;

            // Формируем массивы для UNNEST (один проход по списку)
            var count = list.Count;
            var ids = new Guid[count];
            var tickers = new string[count];
            var prices = new string[count];
            var volumes = new string[count];
            var timestamps = new DateTime[count];
            var exchanges = new string[count];
            var receivedAts = new DateTime[count];
            var normalizeds = new bool[count];

            for (int i = 0; i < count; i++)
            {
                var e = list[i];
                ids[i] = e.Id;
                tickers[i] = e.Ticker;
                prices[i] = e.Price.ToString(CultureInfo.InvariantCulture);
                volumes[i] = e.Volume.ToString(CultureInfo.InvariantCulture);
                timestamps[i] = e.Timestamp;
                exchanges[i] = e.Exchange;
                receivedAts[i] = e.ReceivedAt;
                normalizeds[i] = e.Normalized;
            }

            const string sql = @"
                INSERT INTO rawticks (""id"", ""ticker"", ""price"", ""volume"", ""timestamp"", ""exchange"", ""receivedat"", ""normalized"")
                SELECT unnest(@ids), unnest(@tickers), unnest(@prices::text[])::numeric, unnest(@volumes::text[])::numeric,
                       unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)
                ON CONFLICT (""ticker"", ""exchange"", ""timestamp"") DO NOTHING;";

            var parameters = new Npgsql.NpgsqlParameter[]
            {
                new("@ids", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Uuid) { Value = ids },
                new("@tickers", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = tickers },
                new("@prices", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = prices },
                new("@volumes", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = volumes },
                new("@timestamps", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = timestamps },
                new("@exchanges", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = exchanges },
                new("@receivedats", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = receivedAts },
                new("@normalizeds", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Boolean) { Value = normalizeds },
            };

            // Retry loop — 1 попытка при deadlock (safety-net)
            int attempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);
                }
                catch (Exception ex) when (
                    (ex is PostgresException pgEx && pgEx.SqlState == "40P01" && attempt < 1)
                    || (ex is NpgsqlException && attempt < 1)
                )
                {
                    attempt++;
                    await Task.Delay(200, cancellationToken);
                }
            }
        }

        public async Task<int> GetCountAsync(DateTime? from = null, DateTime? to = null, CancellationToken cancellationToken = default)
        {
            var query = _dbSet.AsQueryable();

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.CountAsync(cancellationToken);
        }

        public async Task<IEnumerable<RawTick>> GetUnnormalizedAsync(int limit = 1000, CancellationToken cancellationToken = default)
        {
            return await _dbSet
                .Where(t => !t.Normalized)
                .OrderBy(t => t.Timestamp)
                .Take(limit)
                .ToListAsync(cancellationToken);
        }
    }
}
