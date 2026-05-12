using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Infrastructure.Repositories
{
    public class ConnectionLogRepository : IConnectionLogRepository
    {
        private readonly MarketDataDbContext _context;
        private readonly DbSet<ConnectionLog> _dbSet;

        public ConnectionLogRepository(MarketDataDbContext context)
        {
            _context = context;
            _dbSet = context.Set<ConnectionLog>();
        }

        public async Task AddAsync(ConnectionLog log, CancellationToken cancellationToken = default)
        {
            await _dbSet.AddAsync(log, cancellationToken);
        }

        public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            await _context.SaveChangesAsync(cancellationToken);
        }
    }
}