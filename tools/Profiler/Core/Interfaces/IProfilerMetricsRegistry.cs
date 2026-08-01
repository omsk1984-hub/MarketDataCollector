namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>
/// Реестр live-метрик профилирования для встроенного HTTP-сервера профайлера.
/// Потокобезопасен: обновляется оркестратором и читается HTTP-хендлером `/health`.
/// </summary>
public interface IProfilerMetricsRegistry
{
    /// <summary>Устанавливает статус сервера/профайлера: <c>healthy</c> или <c>degraded</c>.</summary>
    void SetStatus(string status);

    /// <summary>Устанавливает текущий шаг профилирования.</summary>
    void SetCurrentStep(string currentStep);

    /// <summary>Устанавливает фактическую длительность trace, секунд.</summary>
    void SetTraceDurationSeconds(double seconds);

    /// <summary>Устанавливает флаг успешности первого gcdump (пик).</summary>
    void SetGcDumpPeakSuccess(bool success);

    /// <summary>Устанавливает флаг успешности второго gcdump (drained).</summary>
    void SetGcDumpDrainedSuccess(bool success);

    /// <summary>Устанавливает число сэмплов счётчиков Worker'а.</summary>
    void SetCountersSamples(int samples);

    /// <summary>Устанавливает флаг успешности конвертации в SpeedScope.</summary>
    void SetSpeedScopeSuccess(bool success);

    /// <summary>Возвращает потокобезопасный снимок текущих метрик.</summary>
    ProfilerMetricsSnapshot GetSnapshot();
}
