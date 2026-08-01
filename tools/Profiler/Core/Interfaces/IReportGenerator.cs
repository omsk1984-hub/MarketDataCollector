namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Генерация Markdown-отчёта по результатам профилирования.</summary>
public interface IReportGenerator
{
    /// <summary>Формирует отчёт <c>profiling_report_*.md</c> и возвращает путь к нему.</summary>
    /// <param name="runTimestamp">Метка времени запуска (совпадает с остальными артефактами прогона).</param>
    Task<string> GenerateAsync(
        string outputDir,
        string runTimestamp,
        IReadOnlyList<(string Name, string Path)> outputFiles,
        IReadOnlyList<string> warnings,
        CancellationToken cancellationToken);
}
