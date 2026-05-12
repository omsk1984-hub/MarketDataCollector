using MarketDataCollector.Domain.Entities;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IConnectionLogRepository
    {
        Task AddAsync(ConnectionLog log, CancellationToken cancellationToken = default);
        Task SaveChangesAsync(CancellationToken cancellationToken = default);
    }
}