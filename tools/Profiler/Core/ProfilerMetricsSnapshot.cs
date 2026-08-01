namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Потокобезопасный снимок live-метрик профайлера. DTO через <c>record</c>.
/// </summary>
public sealed record ProfilerMetricsSnapshot(
    string Status,
    string CurrentStep,
    double TraceDurationSeconds,
    bool GcDumpPeakSuccess,
    bool GcDumpDrainedSuccess,
    int CountersSamples,
    bool SpeedScopeSuccess,
    double ElapsedSeconds)
{
    /// <summary>Снимок по умолчанию (сервер только стартовал).</summary>
    public static ProfilerMetricsSnapshot Empty { get; } = new(
        Status: "healthy",
        CurrentStep: "Инициализация",
        TraceDurationSeconds: 0,
        GcDumpPeakSuccess: false,
        GcDumpDrainedSuccess: false,
        CountersSamples: 0,
        SpeedScopeSuccess: false,
        ElapsedSeconds: 0);
}
