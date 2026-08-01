namespace MarketDataCollector.Profiler.Options;

/// <summary>
/// Параметры запуска утилиты профилирования (аналог CLI параметров
/// <c>run_all_metrics.ps1</c> и <c>collect-all.ps1</c>).
/// </summary>
public sealed record ProfilerOptions
{
    /// <summary>Профиль dotnet-trace: gc-verbose, cpu-sampling, contention, contention-cpu.</summary>
    public string TraceProfile { get; init; } = "gc-verbose";

    /// <summary>Длительность trace, секунд.</summary>
    public int TraceDuration { get; init; } = 90;

    /// <summary>Момент первого gcdump (пик), секунд.</summary>
    public int GcDumpAtPeakSec { get; init; } = 50;

    /// <summary>Ожидание дренажа перед вторым gcdump, секунд.</summary>
    public int DrainWaitSec { get; init; } = 30;

    /// <summary>Имя процесса Worker.</summary>
    public string WorkerProcessName { get; init; } = "MarketDataCollector.Worker";

    /// <summary>Prometheus metrics endpoint.</summary>
    public string MetricsUrl { get; init; } = "http://localhost:5010/metrics";

    /// <summary>Health-check endpoint Worker'а.</summary>
    public string HealthUrl { get; init; } = "http://localhost:5010/health";

    /// <summary>Таймаут ожидания healthy, секунд.</summary>
    public int HealthTimeoutSec { get; init; } = 30;

    /// <summary>Директория результатов.</summary>
    public string OutputDir { get; init; } = "./traces";

    /// <summary>Интервал опроса метрик, секунд.</summary>
    public int RefreshSeconds { get; init; } = 5;

    /// <summary>Уровень логирования HTTP-запросов (Trace/Debug/Information/None).</summary>
    public string HttpLogLevel { get; init; } = "Debug";
}
