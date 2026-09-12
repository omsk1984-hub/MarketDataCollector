using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Services;

/// <summary>
/// Оркестратор полного цикла профилирования (режим "all"):
/// Phase 1: dotnet-tools → health → имена файлов → PID → trace → counters → gcdump(peak) → завершение trace
/// Phase 2 (после внешнего сигнала): дренаж → gcdump(drained) → speedscope → отчёт.
/// </summary>
public sealed class ProfilerOrchestrator : IProfilerOrchestrator
{
    private readonly IEnsureDotnetTools _ensureDotnetTools;
    private readonly IHealthCheckService _healthCheckService;
    private readonly IProcessFinder _processFinder;
    private readonly ITraceCollector _traceCollector;
    private readonly ICountersCollector _countersCollector;
    private readonly IPeakLoadWaiter _peakLoadWaiter;
    private readonly IGcDumpCollector _gcDumpCollector;
    private readonly IDrainWaiter _drainWaiter;
    private readonly ISpeedScopeConverter _speedScopeConverter;
    private readonly ITopNReporter _topNReporter;
    private readonly IReportGenerator _reportGenerator;
    private readonly IProfilerMetricsRegistry _metrics;
    private readonly IConsoleUI _ui;
    private readonly ILogger<ProfilerOrchestrator> _logger;
    private readonly ProfilerOptions _options;

    // TCS для двухфазного сбора: Phase 1 завершается, ждёт сигнала от HTTP-сервера.
    private readonly TaskCompletionSource<bool> _drainSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private string _ts = string.Empty;
    private string _peakGcDumpPath = string.Empty;
    private string _drainedGcDumpPath = string.Empty;
    private string _countersPath = string.Empty;
    private int _processId;

    public ProfilerOrchestrator(
        IEnsureDotnetTools ensureDotnetTools,
        IHealthCheckService healthCheckService,
        IProcessFinder processFinder,
        ITraceCollector traceCollector,
        ICountersCollector countersCollector,
        IPeakLoadWaiter peakLoadWaiter,
        IGcDumpCollector gcDumpCollector,
        IDrainWaiter drainWaiter,
        ISpeedScopeConverter speedScopeConverter,
        ITopNReporter topNReporter,
        IReportGenerator reportGenerator,
        IProfilerMetricsRegistry metrics,
        IConsoleUI ui,
        ILogger<ProfilerOrchestrator> logger,
        ProfilerOptions options)
    {
        _ensureDotnetTools = ensureDotnetTools;
        _healthCheckService = healthCheckService;
        _processFinder = processFinder;
        _traceCollector = traceCollector;
        _countersCollector = countersCollector;
        _peakLoadWaiter = peakLoadWaiter;
        _gcDumpCollector = gcDumpCollector;
        _drainWaiter = drainWaiter;
        _speedScopeConverter = speedScopeConverter;
        _topNReporter = topNReporter;
        _reportGenerator = reportGenerator;
        _metrics = metrics;
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public void SignalDrainReady()
    {
        _drainSignal.TrySetResult(true);
    }

    public async Task<int> RunAllAsync(CancellationToken cancellationToken)
    {
        var warnings = new List<string>();

        _metrics.SetCurrentStep("1. Проверка dotnet-инструментов");
        _ui.SectionHeader("1. Проверка dotnet-инструментов");
        await _ensureDotnetTools.EnsureAsync(cancellationToken);

        _metrics.SetCurrentStep("2. Health-check");
        _ui.SectionHeader("2. Health-check");
        await _healthCheckService.WaitForHealthyAsync(_options.HealthUrl, _options.HealthTimeoutSec, cancellationToken);

        _metrics.SetCurrentStep("3. Подготовка выходных файлов");
        _ui.SectionHeader("3. Подготовка выходных файлов");
        DateTime startedAt = DateTime.Now;
        Directory.CreateDirectory(_options.OutputDir);
        _ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");

        string tracePath = Path.Combine(_options.OutputDir, $"allocation_trace_{_ts}.nettrace");
        _peakGcDumpPath = Path.Combine(_options.OutputDir, $"snapshot_peak_{_ts}.gcdump");
        _drainedGcDumpPath = Path.Combine(_options.OutputDir, $"snapshot_drained_{_ts}.gcdump");
        _countersPath = Path.Combine(_options.OutputDir, $"counters_{_ts}.csv");

        _ui.Info($"Trace: {tracePath}");
        _ui.Info($"gcdump(peak): {_peakGcDumpPath}");
        _ui.Info($"gcdump(drained): {_drainedGcDumpPath}");
        _ui.Info($"Counters: {_countersPath}");

        _metrics.SetCurrentStep("4. Поиск процесса Worker");
        _ui.SectionHeader("4. Поиск процесса Worker");
        _processId = _processFinder.FindProcessId(cancellationToken);

        _metrics.SetCurrentStep("5. Запуск trace");
        _ui.SectionHeader("5. Запуск trace");
        TraceRun trace = await _traceCollector.StartAsync(
            _processId, _options.TraceDuration, tracePath, _options.TraceProfile, cancellationToken);

        _metrics.SetCurrentStep("6. Сбор счётчиков");
        _ui.SectionHeader("6. Сбор счётчиков");
        Task countersTask = _countersCollector.StartAsync(_countersPath, cancellationToken);

        _metrics.SetCurrentStep("7. Первый gcdump (пик)");
        _ui.SectionHeader("7. Первый gcdump (пик)");
        await _peakLoadWaiter.WaitForPeakLoadAsync(_options.GcDumpAtPeakSec, cancellationToken);
        GcDumpResult peakResult = await _gcDumpCollector.CollectAsync(
            _processId, _peakGcDumpPath, "PEAK", cancellationToken);

        _metrics.SetGcDumpPeakSuccess(peakResult.FileSizeBytes > 0);
        if (peakResult.FileSizeBytes == 0)
        {
            warnings.Add("Первый gcdump (пик) не создан.");
        }

        _metrics.SetCurrentStep("8. Завершение trace");
        _ui.SectionHeader("8. Завершение trace");
        await _traceCollector.StopAsync(trace, cancellationToken);

        _metrics.SetTraceDurationSeconds((int)(DateTime.Now - startedAt).TotalSeconds);
        if (!File.Exists(trace.OutputPath))
        {
            warnings.Add("Trace-файл не создан.");
        }

        // ============================================================
        // Phase 2: ожидание внешнего сигнала дренажа
        // ============================================================
        _metrics.SetCurrentStep("8.5. Ожидание сигнала дренажа");
        _ui.SectionHeader("8.5. Ожидание сигнала дренажа от оркестратора");

        using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        signalCts.CancelAfter(TimeSpan.FromSeconds(_options.DrainSignalTimeoutSec));

        try
        {
            await _drainSignal.Task.WaitAsync(signalCts.Token);
            _ui.Ok("Получен сигнал дренажа — запуск второй фазы.");
        }
        catch (OperationCanceledException)
        {
            _ui.Warn($"Таймаут ожидания сигнала дренажа ({_options.DrainSignalTimeoutSec}с) — " +
                     $"продолжение по таймауту (фолбэк).");
        }

        // ============================================================
        // Phase 2: дренаж и второй gcdump (drained)
        // ============================================================
        _metrics.SetCurrentStep("9. Дренаж и второй gcdump");
        _ui.SectionHeader("9. Дренаж и второй gcdump");
        await _drainWaiter.WaitForDrainAsync(_options.DrainWaitSec, _options.MetricsUrl, cancellationToken);
        GcDumpResult drainedResult = await _gcDumpCollector.CollectAsync(
            _processId, _drainedGcDumpPath, "DRAINED", cancellationToken);

        _metrics.SetGcDumpDrainedSuccess(drainedResult.FileSizeBytes > 0);
        if (drainedResult.FileSizeBytes == 0)
        {
            warnings.Add("Второй gcdump (drained) не создан.");
        }

        _metrics.SetCurrentStep("10. Конвертация в SpeedScope");
        _ui.SectionHeader("10. Конвертация в SpeedScope");
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        string speedScopePath = await _speedScopeConverter.ConvertAsync(trace.OutputPath, cancellationToken);

        _metrics.SetSpeedScopeSuccess(!string.IsNullOrEmpty(speedScopePath));
        if (string.IsNullOrEmpty(speedScopePath))
        {
            warnings.Add("SpeedScope-файл не создан (возможно, повреждённый trace).");
        }

        _metrics.SetCurrentStep("10.5. topN-анализ (dotnet-trace report topN)");
        _ui.SectionHeader("10.5. topN-анализ (dotnet-trace report topN)");
        string topNPath = string.Empty;
        if (_options.TopNEnabled)
        {
            topNPath = await _topNReporter.GenerateAsync(trace.OutputPath, cancellationToken);
            if (string.IsNullOrEmpty(topNPath))
            {
                warnings.Add("topN-отчёт не создан (trace может не содержать CPU-событий).");
            }
        }
        else
        {
            _ui.Info("topN-анализ отключён (--topn-enabled=false).");
        }

        _metrics.SetCurrentStep("11. Остановка сбора счётчиков");
        _ui.SectionHeader("11. Остановка сбора счётчиков");
        // CountersCollector завершится сам по факту внешней отмены — дожидаемся задачи.
        await Task.WhenAny(countersTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));

        _metrics.SetCurrentStep("12. Генерация отчёта");
        _ui.SectionHeader("12. Генерация отчёта");
        var outputFiles = new List<(string Name, string Path)>
        {
            ("Trace (.nettrace)", trace.OutputPath),
            ("SpeedScope (.json)", speedScopePath),
            ("topN report (.md)", topNPath),
            ("gcdump (peak)", _peakGcDumpPath),
            ("gcdump (drained)", _drainedGcDumpPath),
            ("Counters (.csv)", _countersPath),
        };

        string reportPath = await _reportGenerator.GenerateAsync(_options.OutputDir, _ts, outputFiles, warnings, cancellationToken);

        _metrics.SetCurrentStep("Завершено");
        _ui.SectionHeader("Итог");
        _ui.Ok($"Отчёт: {reportPath}");
        _ui.Info($"Проверьте артефакты в: {Path.GetFullPath(_options.OutputDir)}");

        return 0;
    }
}
