using System.Diagnostics;
using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;

namespace MarketDataCollector.Profiler.Services;

/// <summary>
/// Потокобезопасная реализация реестра live-метрик профайлера.
/// Все мутации защищены <c>lock</c>; чтение отдаёт снимок (record) одним куском.
/// </summary>
public sealed class ProfilerMetricsRegistry : IProfilerMetricsRegistry
{
    private readonly object _lock = new();
    private readonly Stopwatch _sw = Stopwatch.StartNew();

    private string _status = ProfilerMetricsSnapshot.Empty.Status;
    private string _currentStep = ProfilerMetricsSnapshot.Empty.CurrentStep;
    private double _traceDurationSeconds;
    private bool _gcDumpPeakSuccess;
    private bool _gcDumpDrainedSuccess;
    private int _countersSamples;
    private bool _speedScopeSuccess;

    public void SetStatus(string status)
    {
        ArgumentNullException.ThrowIfNull(status);

        lock (_lock)
        {
            _status = status;
        }
    }

    public void SetCurrentStep(string currentStep)
    {
        ArgumentNullException.ThrowIfNull(currentStep);

        lock (_lock)
        {
            _currentStep = currentStep;
        }
    }

    public void SetTraceDurationSeconds(double seconds)
    {
        lock (_lock)
        {
            _traceDurationSeconds = seconds;
        }
    }

    public void SetGcDumpPeakSuccess(bool success)
    {
        lock (_lock)
        {
            _gcDumpPeakSuccess = success;
        }
    }

    public void SetGcDumpDrainedSuccess(bool success)
    {
        lock (_lock)
        {
            _gcDumpDrainedSuccess = success;
        }
    }

    public void SetCountersSamples(int samples)
    {
        lock (_lock)
        {
            _countersSamples = samples;
        }
    }

    public void SetSpeedScopeSuccess(bool success)
    {
        lock (_lock)
        {
            _speedScopeSuccess = success;
        }
    }

    public ProfilerMetricsSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new ProfilerMetricsSnapshot(
                Status: _status,
                CurrentStep: _currentStep,
                TraceDurationSeconds: _traceDurationSeconds,
                GcDumpPeakSuccess: _gcDumpPeakSuccess,
                GcDumpDrainedSuccess: _gcDumpDrainedSuccess,
                CountersSamples: _countersSamples,
                SpeedScopeSuccess: _speedScopeSuccess,
                ElapsedSeconds: _sw.Elapsed.TotalSeconds);
        }
    }
}
