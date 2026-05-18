using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
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
            return await _context.Database.ExecuteSqlRawAsync(formattedSql, parameters, cancellationToken);
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
