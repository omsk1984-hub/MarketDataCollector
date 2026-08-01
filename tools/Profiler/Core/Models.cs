using System.Diagnostics;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Результат запуска внешнего инструмента (dotnet-trace / dotnet-gcdump).
/// DTO через <c>record</c>.
/// </summary>
public sealed record ToolRun(
    Process Process,
    string OutputPath,
    Task<string> StdOutTask,
    Task<string> StdErrTask);

/// <summary>Запущенный сеанс сбора trace.</summary>
public sealed record TraceRun(
    ToolRun Run,
    int ProcessId,
    string OutputPath,
    string Profile);

/// <summary>Результат сбора дампа кучи (gcdump).</summary>
public sealed record GcDumpResult(
    string OutputPath,
    long FileSizeBytes,
    int ExitCode);

/// <summary>Кандидат на процесс Worker, найденный одним из источников.</summary>
public sealed record ProcessIdCandidate(
    int ProcessId,
    string Name,
    int Priority);

/// <summary>Разобранный образец Prometheus-метрики.</summary>
public sealed record MetricSample(
    string Timestamp,
    string Metric,
    string Labels,
    string Type,
    string Value,
    string Description);
