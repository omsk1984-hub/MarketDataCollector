namespace MarketDataCollector.Profiler.Core;

/// <summary>Метрики, отдаваемые встроенным HTTP-сервером в ответе `/health`.</summary>
public sealed record ProfilerMetricsPayload(
    double TraceDurationSeconds,
    bool GcDumpPeakSuccess,
    bool GcDumpDrainedSuccess,
    int CountersSamples,
    bool SpeedScopeSuccess,
    double ElapsedSeconds);

/// <summary>Ответ встроенного `/health` профайлера (JSON, camelCase).</summary>
public sealed record ProfilerHealthResponse(
    string Status,
    string CurrentStep,
    ProfilerMetricsPayload Metrics,
    string Timestamp);
