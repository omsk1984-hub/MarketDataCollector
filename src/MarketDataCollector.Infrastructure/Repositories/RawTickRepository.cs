using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Infrastructure.Repositories
{
    public class RawTickRepository : IRawTickRepository
    {
        private readonly MarketDataDbContext _context;
        private readonly DbSet<RawTick> _dbSet;
        private readonly ILogger<RawTickRepository> _logger;

        // Reusable NpgsqlParameter[] for BulkCopyAsync(TickData) — pre-allocated once,
        // avoids 8× new NpgsqlParameter per batch (~8.5 batches/sec).
        // Safe because RawTickRepository is Scoped + consumer processes batches sequentially.
        private readonly Npgsql.NpgsqlParameter[] _tickDataParameters;
        private const string SqlTickDataBulkCopy = @"
            INSERT INTO rawticks (""id"", ""ticker"", ""price"", ""volume"", ""timestamp"", ""exchange"", ""receivedat"", ""normalized"")
            SELECT unnest(@ids), unnest(@tickers), unnest(@prices), unnest(@volumes),
                   unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)
            ON CONFLICT (""ticker"", ""exchange"", ""timestamp"") DO NOTHING;";

        // Reusable arrays for BulkCopyAsync(TickData) — zero per-batch allocations on steady state.
        // Safe because RawTickRepository is Scoped + consumer processes batches sequentially.
        // Cannot use ArrayPool directly because Npgsql requires precise Array.Length,
        // and Rent() may return a larger array.
        private Guid[]? _idsCache;
        private string[]? _tickersCache;
        private decimal[]? _pricesCache;
        private decimal[]? _volumesCache;
        private DateTime[]? _timestampsCache;
        private string[]? _exchangesCache;
        private DateTime[]? _receivedAtsCache;
        private bool[]? _normalizedsCache;

        public RawTickRepository(MarketDataDbContext context, ILogger<RawTickRepository> logger)
        {
            _context = context;
            _dbSet = context.Set<RawTick>();
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            // Pre-create the NpgsqlParameter array — only Value is updated per batch call
            _tickDataParameters = new Npgsql.NpgsqlParameter[]
            {
                new("@ids", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Uuid) { Value = null! },
                new("@tickers", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = null! },
                new("@prices", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Numeric) { Value = null! },
                new("@volumes", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Numeric) { Value = null! },
                new("@timestamps", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = null! },
                new("@exchanges", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = null! },
                new("@receivedats", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = null! },
                new("@normalizeds", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Boolean) { Value = null! },
            };
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
        /// Количество повторов для BulkCopy при транзиентных ошибках (deadlock, timeout, network).
        /// Экспоненциальный backoff: 100ms, 200ms, 400ms (+ jitter 0-100ms).
        /// </summary>
        private const int BulkCopyMaxRetries = 3;

        /// <summary>
        /// Базовая задержка между retry при транзиентных ошибках (экспоненциальная: 100ms, 200ms, 400ms).
        /// </summary>
        private static readonly TimeSpan BulkCopyBaseDelay = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Источник случайных чисел для jitter. Shared между всеми экземплярами,
        /// т.к. Random не thread-safe — используем ThreadLocal.
        /// </summary>
        private static readonly ThreadLocal<Random> JitterRandom = new(() => new Random());

        /// <summary>
        /// Определяет, является ли исключение транзиентным (можно повторить операцию).
        /// </summary>
        private static bool IsTransient(Exception ex)
        {
            return (ex is PostgresException pg && pg.SqlState is "40P01" or "57014" or "08006" or "08001" or "08003")
                || ex is NpgsqlException
                || ex is TimeoutException;
        }

        /// <summary>
        /// Returns a reusable array of exactly <paramref name="count"/> elements.
        /// Allocates a new array only when <paramref name="count"/> changes (rare).
        /// Thread-safe only when called sequentially (Scoped repository, single consumer).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static T[] RentOrCreate<T>([MaybeNull] ref T[]? cache, int count)
        {
            if (cache == null || cache.Length != count)
                cache = new T[count];
            return cache;
        }

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

            // Retry loop для транзиентных ошибок (deadlock, timeout, connection)
            int attempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _context.Database.ExecuteSqlRawAsync(formattedSql, parameters, cancellationToken);
                }
                catch (Exception ex) when (IsTransient(ex) && attempt < BulkCopyMaxRetries)
                {
                    attempt++;
                    var delay = BulkCopyBaseDelay * (int)Math.Pow(2, attempt - 1);
                    var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next(100));
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
        /// Retry: safety-net для транзиентных ошибок Npgsql (deadlock, timeout, network).
        /// Deadlock'и (40P01) невозможны — per-ticker routing в MarketDataProcessor
        /// гарантирует disjoint наборы тикеров между consumer'ами,
        /// поэтому два INSERT'а никогда не конкурируют за один unique index.
        /// </summary>
        public async Task<int> BulkCopyAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default)
        {
            var list = entities.ToList();
            if (list.Count == 0)
                return 0;

            // Формируем массивы для UNNEST (один проход по списку)
            var count = list.Count;
            // Используем прямые new[] для Npgsql — массивы <85 KB, не LOH.
            // ArrayPool не подходит: Npgsql требует точного размера массива (Array.Length),
            // а Rent() может вернуть массив больше запрошенного размера.
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

            // Retry loop с экспоненциальным backoff + jitter для транзиентных ошибок
            int attempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);
                }
                catch (Exception ex) when (IsTransient(ex) && attempt < BulkCopyMaxRetries)
                {
                    attempt++;
                    var delay = BulkCopyBaseDelay * (int)Math.Pow(2, attempt - 1);
                    var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next(100));
                    _logger.LogWarning(ex,
                        "BulkCopy (RawTick) attempt {Attempt}/{MaxRetries} failed with {ExceptionType}, " +
                        "SqlState={SqlState}, retrying after {Delay}ms, count={Count}",
                        attempt, BulkCopyMaxRetries, ex.GetType().Name,
                        (ex is PostgresException pg ? pg.SqlState : null),
                        (delay + jitter).TotalMilliseconds, count);
                    await Task.Delay(delay + jitter, cancellationToken);
                }
            }
        }

        /// <summary>
        /// Generates a UUID v7 (time-ordered) for better b-tree index clustering.
        /// Format: 48-bit Unix ms timestamp (big-endian) + 74 random bits.
        /// Uses thread-local Random for the random portion to avoid locks in hot path.
        /// </summary>
        private static readonly ThreadLocal<Random> UuidRandom = new(() => new Random());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Guid NextUuidV7()
        {
            // UUID v7: 48-bit timestamp (milliseconds since Unix epoch) + 74 random bits
            // Layout: [32-bit ms][16-bit ms][16-bit random][64-bit random]
            var ms = (long)(DateTime.UtcNow - DateTime.UnixEpoch).TotalMilliseconds;
            var random = UuidRandom.Value!;

            // Generate 10 random bytes (80 bits, but we only use 74)
            Span<byte> guidBytes = stackalloc byte[16];
            random.NextBytes(guidBytes.Slice(6)); // fill last 10 bytes with randomness

            // Set version (7) in the 4 most significant bits of byte 7 (guid[6] in network order)
            // guid[6] = (guid[6] & 0x0F) | 0x70; (version 7)
            guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x70);

            // Set variant (RFC 4122) in byte 8: 10xx xxxx
            guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);

            // Write 48-bit timestamp (big-endian) in bytes 0-5
            guidBytes[0] = (byte)((ms >> 40) & 0xFF);
            guidBytes[1] = (byte)((ms >> 32) & 0xFF);
            guidBytes[2] = (byte)((ms >> 24) & 0xFF);
            guidBytes[3] = (byte)((ms >> 16) & 0xFF);
            guidBytes[4] = (byte)((ms >> 8) & 0xFF);
            guidBytes[5] = (byte)(ms & 0xFF);

            return new Guid(guidBytes);
        }

        /// <summary>
        /// Bulk insert напрямую из IReadOnlyList<TickData> без промежуточного List<RawTick>.
        /// Создаёт RawTick в одном проходе — устраняет двойную итерацию и лишние аллокации.
        /// Price/Volume передаются как numeric[] (было text[], теперь decimal[] без string аллокаций).
        /// Использует UUID v7 для кластеризованных b-tree индексов.
        /// </summary>
        public async Task<int> BulkCopyAsync(IReadOnlyList<TickData> ticks, ITimeService timeService, CancellationToken cancellationToken = default)
        {
            if (ticks.Count == 0)
                return 0;

            var count = ticks.Count;

            // Reusable cached buffers — zero per-batch allocations on steady state.
            // ArrayPool.Rent() may return a larger array than requested, which breaks
            // Npgsql because it uses Array.Length as element count. Instead, we cache
            // arrays and reallocate only when batch size changes (rare).
            var ids = RentOrCreate(ref _idsCache, count);
            var tickers = RentOrCreate(ref _tickersCache, count);
            var prices = RentOrCreate(ref _pricesCache, count);
            var volumes = RentOrCreate(ref _volumesCache, count);
            var timestamps = RentOrCreate(ref _timestampsCache, count);
            var exchanges = RentOrCreate(ref _exchangesCache, count);
            var receivedAts = RentOrCreate(ref _receivedAtsCache, count);
            var normalizeds = RentOrCreate(ref _normalizedsCache, count);

            var now = timeService.UtcNow;

            for (int i = 0; i < count; i++)
            {
                var t = ticks[i];
                ids[i] = NextUuidV7();          // UUID v7 instead of Guid.NewGuid()
                tickers[i] = t.Ticker;
                prices[i] = t.Price;
                volumes[i] = t.Volume;
                timestamps[i] = t.Timestamp;
                exchanges[i] = t.Exchange;
                receivedAts[i] = now;
                normalizeds[i] = false;
            }

            // Reuse pre-allocated NpgsqlParameter[] — only update Value each batch call
            _tickDataParameters[0].Value = ids;
            _tickDataParameters[1].Value = tickers;
            _tickDataParameters[2].Value = prices;
            _tickDataParameters[3].Value = volumes;
            _tickDataParameters[4].Value = timestamps;
            _tickDataParameters[5].Value = exchanges;
            _tickDataParameters[6].Value = receivedAts;
            _tickDataParameters[7].Value = normalizeds;

            int attempt = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    return await _context.Database.ExecuteSqlRawAsync(SqlTickDataBulkCopy, _tickDataParameters, cancellationToken);
                }
                catch (Exception ex) when (IsTransient(ex) && attempt < BulkCopyMaxRetries)
                {
                    attempt++;
                    var delay = BulkCopyBaseDelay * (int)Math.Pow(2, attempt - 1);
                    var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next(100));
                    _logger.LogWarning(ex,
                        "BulkCopy (TickData) attempt {Attempt}/{MaxRetries} failed with {ExceptionType}, " +
                        "SqlState={SqlState}, retrying after {Delay}ms, count={Count}",
                        attempt, BulkCopyMaxRetries, ex.GetType().Name,
                        (ex is PostgresException pg ? pg.SqlState : null),
                        (delay + jitter).TotalMilliseconds, count);
                    await Task.Delay(delay + jitter, cancellationToken);
                }
                catch (Exception ex) when (!IsTransient(ex) || attempt >= BulkCopyMaxRetries)
                {
                    _logger.LogError(ex,
                        "BulkCopy (TickData) failed permanently after {Attempt}/{MaxRetries} attempts, count={Count}",
                        attempt, BulkCopyMaxRetries, count);
                    throw;
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
