using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
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
        /// Семафор для сериализации всех вызовов BulkCopyAsync.
        ///
        /// Root cause deadlock'ов: несколько параллельных consumer'ов одновременно
        /// вставляют данные через temp table + INSERT ... ON CONFLICT DO NOTHING
        /// в таблицу rawticks с unique-индексом (ticker, exchange, timestamp).
        /// PostgreSQL's B-tree index page-level блокировки приводят к взаимоблокировке
        /// при конкурентной вставке записей, попадающих в одни и те же страницы индекса.
        ///
        /// SemaphoreSlim(1,1) гарантирует, что только один поток одновременно выполняет
        /// DROP/CREATE temp table, Binary COPY и INSERT ... ON CONFLICT.
        /// Это полностью устраняет deadlock'и на уровне БД.
        ///
        /// Retry-логика (5 попыток) остаётся safety net'ом на случай других транзиентных
        /// ошибок (timeout, serialization failures).
        /// </summary>
        private static readonly SemaphoreSlim BulkCopyLock = new(1, 1);

        /// <summary>
        /// Источник случайных чисел для jitter. Shared между всеми экземплярами,
        /// т.к. Random не thread-safe — используем ThreadLocal.
        /// </summary>
        private static readonly ThreadLocal<Random> JitterRandom = new(() => new Random());

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
        /// Bulk insert через Binary COPY protocol (Npgsql) + temp table + ON CONFLICT DO NOTHING.
        /// 
        /// ВАЖНО: Временная таблица DROP'ается и CREATE'ится заново при каждом вызове,
        /// чтобы избежать накопления данных из предыдущих вызовов (что может произойти
        /// при использовании No Reset On Close=true и retry-логики).
        /// 
        /// Retry-логика обрабатывает:
        /// - deadlock detected (40P01) — транзиентные конфликты блокировок
        /// - TimeoutException — долгие запросы при возросшем объёме данных
        /// </summary>
        public async Task<int> BulkCopyAsync(IEnumerable<RawTick> entities, CancellationToken cancellationToken = default)
        {
            var list = entities.ToList();
            if (list.Count == 0)
                return 0;

            // Сериализуем доступ к binary COPY + INSERT ON CONFLICT через семафор.
            // Root cause deadlock'ов: конкуренция за page-level блокировки B-tree
            // индекса unique_tick при параллельной вставке из 4 consumer'ов.
            // SemaphoreSlim(1,1) гарантирует выполнение только одним потоком.
            await BulkCopyLock.WaitAsync(cancellationToken);
            try
            {
                // Retry loop для транзиентных deadlock'ов (40P01) и timeouts
                int attempt = 0;
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var conn = (NpgsqlConnection)_context.Database.GetDbConnection();
                    var needOpen = conn.State != System.Data.ConnectionState.Open;
                    if (needOpen)
                        await conn.OpenAsync(cancellationToken);

                    try
                    {
                        // 1. Пересоздаём временную таблицу (DROP + CREATE вместо CREATE IF NOT EXISTS)
                        //    Это гарантирует чистый стейт, даже если No Reset On Close=true сохранил
                        //    старые данные в temp-таблице от предыдущего вызова.
                        await using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = @"
                                DROP TABLE IF EXISTS rawticks_staging;
                                CREATE TEMP TABLE rawticks_staging (
                                    id UUID,
                                    ticker VARCHAR(20),
                                    price DECIMAL(18,8),
                                    volume DECIMAL(18,8),
                                    timestamp TIMESTAMPTZ,
                                    exchange VARCHAR(50),
                                    receivedat TIMESTAMPTZ,
                                    normalized BOOLEAN
                                );";
                            await cmd.ExecuteNonQueryAsync(cancellationToken);
                        }

                        // 2. Binary COPY во временную таблицу
                        await using (var writer = conn.BeginBinaryImport(
                            "COPY rawticks_staging (id, ticker, price, volume, timestamp, exchange, receivedat, normalized) FROM STDIN (FORMAT BINARY)"))
                        {
                            for (int i = 0; i < list.Count; i++)
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                writer.StartRow();
                                writer.Write(list[i].Id, NpgsqlTypes.NpgsqlDbType.Uuid);
                                writer.Write(list[i].Ticker, NpgsqlTypes.NpgsqlDbType.Varchar);
                                writer.Write(list[i].Price, NpgsqlTypes.NpgsqlDbType.Numeric);
                                writer.Write(list[i].Volume, NpgsqlTypes.NpgsqlDbType.Numeric);
                                writer.Write(list[i].Timestamp, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                                writer.Write(list[i].Exchange, NpgsqlTypes.NpgsqlDbType.Varchar);
                                writer.Write(list[i].ReceivedAt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
                                writer.Write(list[i].Normalized, NpgsqlTypes.NpgsqlDbType.Boolean);
                            }
                            await writer.CompleteAsync(cancellationToken);
                        }

                        // 3. INSERT INTO rawticks ... ON CONFLICT DO NOTHING из временной таблицы
                        //    Увеличиваем CommandTimeout до 120 секунд, т.к. при retry после deadlock'а
                        //    объём данных может быть больше обычного.
                        await using (var cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = @"
                                WITH inserted AS (
                                    INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized)
                                    SELECT id, ticker, price, volume, timestamp, exchange, receivedat, normalized
                                    FROM rawticks_staging
                                    ON CONFLICT (ticker, exchange, timestamp) DO NOTHING
                                    RETURNING 1
                                )
                                SELECT COUNT(*) FROM inserted;";
                            cmd.CommandTimeout = 120;
                            var result = await cmd.ExecuteScalarAsync(cancellationToken);
                            return result is int i ? i : Convert.ToInt32(result);
                        }
                    }
                    catch (Exception ex) when (
                        (ex is PostgresException pgEx && pgEx.SqlState == "40P01" && attempt < DeadlockMaxRetries)
                        || (ex is NpgsqlException && attempt < DeadlockMaxRetries)
                    )
                    {
                        attempt++;

                        // При retry закрываем соединение — temp-таблица будет пересоздана
                        // через DROP + CREATE при следующей попытке
                        if (conn.State == System.Data.ConnectionState.Open || conn.State == System.Data.ConnectionState.Broken)
                        {
                            try { await conn.CloseAsync(); } catch { /* игнорируем */ }
                        }

                        var delay = DeadlockBaseDelay * (int)Math.Pow(2, attempt - 1);
                        // Jitter для предотвращения thundering herd
                        var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next((int)DeadlockMaxJitter.TotalMilliseconds));
                        await Task.Delay(delay + jitter, cancellationToken);
                    }
                    finally
                    {
                        if (needOpen && conn.State == System.Data.ConnectionState.Open)
                            await conn.CloseAsync();
                    }
                }
            }
            finally
            {
                BulkCopyLock.Release();
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
