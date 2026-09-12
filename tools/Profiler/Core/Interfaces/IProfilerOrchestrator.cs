namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Оркестратор полного цикла профилирования (режим "all").</summary>
public interface IProfilerOrchestrator
{
    /// <summary>Выполняет полный цикл профилирования (Phase 1: trace + counters + peak gcdump).</summary>
    Task<int> RunAllAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Сигнализирует оркестратору, что внешний оркестратор (start_loadtest.ps1)
    /// завершил генерацию нагрузки — можно начинать Phase 2 (drain + drained gcdump).
    /// </summary>
    void SignalDrainReady();
}
