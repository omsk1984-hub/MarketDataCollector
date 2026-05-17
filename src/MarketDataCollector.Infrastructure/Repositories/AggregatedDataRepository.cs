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
    public class AggregatedDataRepository : IAggregatedDataRepository
    {
        private readonly MarketDataDbContext _context;
        private readonly DbSet<AggregatedData> _dbSet;

        public AggregatedDataRepository(MarketDataDbContext context)
        {
            _context = context;
            _dbSet = context.Set<AggregatedData>();
        }

        public async Task<AggregatedData?> GetByIdAsync(Guid id, CancellationToken cancellationToken = default)
        {
            return await _dbSet.FindAsync(new object[] { id }, cancellationToken);
        }

        public async Task<IEnumerable<AggregatedData>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            return await _dbSet.ToListAsync(cancellationToken);
        }

        public async Task<IEnumerable<AggregatedData>> FindAsync(Expression<Func<AggregatedData, bool>> predicate, CancellationToken cancellationToken = default)
        {
            return await _dbSet.Where(predicate).ToListAsync(cancellationToken);
        }

        public async Task AddAsync(AggregatedData entity, CancellationToken cancellationToken = default)
        {
            await _dbSet.AddAsync(entity, cancellationToken);
        }

        public async Task AddRangeAsync(IEnumerable<AggregatedData> entities, CancellationToken cancellationToken = default)
        {
            await _dbSet.AddRangeAsync(entities, cancellationToken);
        }

        public void Update(AggregatedData entity)
        {
            _dbSet.Update(entity);
        }

        public void Remove(AggregatedData entity)
        {
            _dbSet.Remove(entity);
        }

        public async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            return await _context.SaveChangesAsync(cancellationToken);
        }

        public async Task<IEnumerable<AggregatedData>> GetByTickerAndIntervalAsync(
            string ticker, string interval,
            DateTime? from = null, DateTime? to = null,
            CancellationToken cancellationToken = default)
        {
            var query = _dbSet.Where(a => a.Ticker == ticker && a.Interval == interval);

            if (from.HasValue)
                query = query.Where(a => a.StartTime >= from.Value);

            if (to.HasValue)
                query = query.Where(a => a.StartTime <= to.Value);

            return await query.OrderBy(a => a.StartTime).ToListAsync(cancellationToken);
        }
    }
}