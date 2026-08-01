using MarketDataCollector.Profiler.Core.Interfaces;

namespace MarketDataCollector.Profiler.Core.ProcessIdSources;

/// <summary>
/// Источник-заглушка для платформ, где WMI недоступен (не Windows).
/// Никогда не возвращает кандидата.
/// </summary>
public sealed class NullProcessSource : IProcessIdSource
{
    public int Priority => 3;

    public Task<ProcessIdCandidate?> TryFindAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<ProcessIdCandidate?>(null);
    }
}
