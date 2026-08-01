namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Оркестратор полного цикла профилирования (режим "all").</summary>
public interface IProfilerOrchestrator
{
    /// <summary>Выполняет полный цикл профилирования.</summary>
    Task<int> RunAllAsync(CancellationToken cancellationToken);
}
