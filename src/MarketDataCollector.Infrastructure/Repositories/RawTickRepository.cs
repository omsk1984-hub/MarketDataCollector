using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

        public async Task<RawTick?> GetByIdAsync(Guid id)
        {
            return await _dbSet.FindAsync(id);
        }

        public async Task<IEnumerable<RawTick>> GetAllAsync()
        {
            return await _dbSet.ToListAsync();
        }

        public async Task<IEnumerable<RawTick>> FindAsync(Expression<Func<RawTick, bool>> predicate)
        {
            return await _dbSet.Where(predicate).ToListAsync();
        }

        public async Task AddAsync(RawTick entity)
        {
            await _dbSet.AddAsync(entity);
        }

        public async Task AddRangeAsync(IEnumerable<RawTick> entities)
        {
            await _dbSet.AddRangeAsync(entities);
        }

        public void Update(RawTick entity)
        {
            _dbSet.Update(entity);
        }

        public void Remove(RawTick entity)
        {
            _dbSet.Remove(entity);
        }

        public async Task<int> SaveChangesAsync()
        {
            return await _context.SaveChangesAsync();
        }

        public async Task<IEnumerable<RawTick>> GetByTickerAsync(string ticker, DateTime? from = null, DateTime? to = null)
        {
            var query = _dbSet.Where(t => t.Ticker == ticker);

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.OrderBy(t => t.Timestamp).ToListAsync();
        }

        public async Task<IEnumerable<RawTick>> GetByExchangeAsync(string exchange, DateTime? from = null, DateTime? to = null)
        {
            var query = _dbSet.Where(t => t.Exchange == exchange);

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.OrderBy(t => t.Timestamp).ToListAsync();
        }

        public async Task<bool> ExistsAsync(string ticker, string exchange, DateTime timestamp)
        {
            return await _dbSet.AnyAsync(t =>
                t.Ticker == ticker &&
                t.Exchange == exchange &&
                t.Timestamp == timestamp);
        }

        public async Task<int> GetCountAsync(DateTime? from = null, DateTime? to = null)
        {
            var query = _dbSet.AsQueryable();

            if (from.HasValue)
                query = query.Where(t => t.Timestamp >= from.Value);

            if (to.HasValue)
                query = query.Where(t => t.Timestamp <= to.Value);

            return await query.CountAsync();
        }

        public async Task<IEnumerable<RawTick>> GetUnnormalizedAsync(int limit = 1000)
        {
            return await _dbSet
                .Where(t => !t.Normalized)
                .OrderBy(t => t.Timestamp)
                .Take(limit)
                .ToListAsync();
        }
    }
}