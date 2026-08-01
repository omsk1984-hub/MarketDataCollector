namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Координатор поиска PID процесса Worker по источникам.</summary>
public interface IProcessFinder
{
    /// <summary>Находит PID процесса. При отсутствии — завершает процесс кодом 1.</summary>
    int FindProcessId(CancellationToken cancellationToken);
}
