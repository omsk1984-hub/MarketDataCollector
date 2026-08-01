namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Сбор дампа управляемой кучи через dotnet-gcdump.</summary>
public interface IGcDumpCollector
{
    /// <summary>Собирает gcdump для процесса в указанный файл.</summary>
    Task<GcDumpResult> CollectAsync(int processId, string outputPath, string label, CancellationToken cancellationToken);
}
