namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>
/// Стратегия поиска PID процесса Worker (OCP). Источники перебираются по приоритету.
/// </summary>
public interface IProcessIdSource
{
    /// <summary>Приоритет источника (меньше — раньше).</summary>
    int Priority { get; }

    /// <summary>Пытается найти кандидата. Возвращает null, если источник не сработал.</summary>
    Task<ProcessIdCandidate?> TryFindAsync(CancellationToken cancellationToken);
}
