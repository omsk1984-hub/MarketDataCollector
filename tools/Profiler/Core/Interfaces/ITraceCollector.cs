namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Управление сбором trace через dotnet-trace.</summary>
public interface ITraceCollector
{
    /// <summary>Запускает сбор trace для процесса.</summary>
    Task<TraceRun> StartAsync(int processId, int durationSec, string outputPath, string profile, CancellationToken cancellationToken);

    /// <summary>Останавливает сбор trace и проверяет результат.</summary>
    Task StopAsync(TraceRun trace, CancellationToken cancellationToken);
}
