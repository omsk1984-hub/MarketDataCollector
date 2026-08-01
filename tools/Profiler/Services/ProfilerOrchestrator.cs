using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Services;

/// <summary>
/// Оркестратор полного цикла профилирования (режим "all"):
/// dotnet-tools → health → имена файлов → PID → trace → counters → gcdump(peak)
/// → завершение trace → дренаж → gcdump(drained) → speedscope → отчёт.
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
    private readonly IReportGenerator _reportGenerator;
    private readonly IConsoleUI _ui;
    private readonly ILogger<ProfilerOrchestrator> _logger;
    private readonly ProfilerOptions _options;

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
        IReportGenerator reportGenerator,
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
        _reportGenerator = reportGenerator;
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public async Task<int> RunAllAsync(CancellationToken cancellationToken)
    {
        var warnings = new List<string>();

        _ui.SectionHeader("1. Проверка dotnet-инструментов");
        await _ensureDotnetTools.EnsureAsync(cancellationToken);

        _ui.SectionHeader("2. Health-check");
        await _healthCheckService.WaitForHealthyAsync(_options.HealthUrl, _options.HealthTimeoutSec, cancellationToken);

        _ui.SectionHeader("3. Подготовка выходных файлов");
        Directory.CreateDirectory(_options.OutputDir);
        string ts = DateTime.Now.ToString("yyyyMMdd_HHmmss");

        string tracePath = Path.Combine(_options.OutputDir, $"allocation_trace_{ts}.nettrace");
        string peakGcDumpPath = Path.Combine(_options.OutputDir, $"snapshot_peak_{ts}.gcdump");
        string drainedGcDumpPath = Path.Combine(_options.OutputDir, $"snapshot_drained_{ts}.gcdump");
        string countersPath = Path.Combine(_options.OutputDir, $"counters_{ts}.csv");

        _ui.Info($"Trace: {tracePath}");
        _ui.Info($"gcdump(peak): {peakGcDumpPath}");
        _ui.Info($"gcdump(drained): {drainedGcDumpPath}");
        _ui.Info($"Counters: {countersPath}");

        _ui.SectionHeader("4. Поиск процесса Worker");
        int processId = _processFinder.FindProcessId(cancellationToken);

        _ui.SectionHeader("5. Запуск trace");
        TraceRun trace = await _traceCollector.StartAsync(
            processId, _options.TraceDuration, tracePath, _options.TraceProfile, cancellationToken);

        _ui.SectionHeader("6. Сбор счётчиков");
        Task countersTask = _countersCollector.StartAsync(countersPath, cancellationToken);

        _ui.SectionHeader("7. Первый gcdump (пик)");
        await _peakLoadWaiter.WaitForPeakLoadAsync(_options.GcDumpAtPeakSec, cancellationToken);
        GcDumpResult peakResult = await _gcDumpCollector.CollectAsync(
            processId, peakGcDumpPath, "PEAK", cancellationToken);

        if (peakResult.FileSizeBytes == 0)
        {
            warnings.Add("Первый gcdump (пик) не создан.");
        }

        _ui.SectionHeader("8. Завершение trace");
        int remainingSec = Math.Max(0, _options.TraceDuration - _options.GcDumpAtPeakSec);
        _ui.Info($"Ожидание завершения trace: ~{remainingSec}с ...");
        await _traceCollector.StopAsync(trace, cancellationToken);

        if (!File.Exists(trace.OutputPath))
        {
            warnings.Add("Trace-файл не создан.");
        }

        _ui.SectionHeader("9. Дренаж и второй gcdump");
        await _drainWaiter.WaitForDrainAsync(_options.DrainWaitSec, _options.MetricsUrl, cancellationToken);
        GcDumpResult drainedResult = await _gcDumpCollector.CollectAsync(
            processId, drainedGcDumpPath, "DRAINED", cancellationToken);

        if (drainedResult.FileSizeBytes == 0)
        {
            warnings.Add("Второй gcdump (drained) не создан.");
        }

        _ui.SectionHeader("10. Конвертация в SpeedScope");
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        string speedScopePath = await _speedScopeConverter.ConvertAsync(trace.OutputPath, cancellationToken);

        if (string.IsNullOrEmpty(speedScopePath))
        {
            warnings.Add("SpeedScope-файл не создан (возможно, повреждённый trace).");
        }

        _ui.SectionHeader("11. Остановка сбора счётчиков");
        // CountersCollector завершится сам по факту внешней отмены — дожидаемся задачи.
        await Task.WhenAny(countersTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));

        _ui.SectionHeader("12. Генерация отчёта");
        var outputFiles = new List<(string Name, string Path)>
        {
            ("Trace (.nettrace)", trace.OutputPath),
            ("SpeedScope (.json)", speedScopePath),
            ("gcdump (peak)", peakGcDumpPath),
            ("gcdump (drained)", drainedGcDumpPath),
            ("Counters (.csv)", countersPath),
        };

        string reportPath = await _reportGenerator.GenerateAsync(_options.OutputDir, outputFiles, warnings, cancellationToken);

        _ui.SectionHeader("Итог");
        _ui.Ok($"Отчёт: {reportPath}");
        _ui.Info($"Проверьте артефакты в: {Path.GetFullPath(_options.OutputDir)}");

        return 0;
    }
}
