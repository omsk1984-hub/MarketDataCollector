using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Управление сбором trace через dotnet-trace.</summary>
public sealed class TraceCollector : ITraceCollector
{
    private const int GracefulStopTimeoutSec = 10;

    private readonly IToolRunner _toolRunner;
    private readonly IConsoleUI _ui;
    private readonly ILogger<TraceCollector> _logger;

    public TraceCollector(IToolRunner toolRunner, IConsoleUI ui, ILogger<TraceCollector> logger)
    {
        _toolRunner = toolRunner;
        _ui = ui;
        _logger = logger;
    }

    public async Task<TraceRun> StartAsync(
        int processId,
        int durationSec,
        string outputPath,
        string profile,
        CancellationToken cancellationToken)
    {
        string args = BuildArgs(processId, outputPath, profile, durationSec);

        _ui.Info($"Запуск dotnet-trace (PID {processId}, {durationSec}с, профиль {profile}) ...");
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", args, cancellationToken);

        return new TraceRun(run, processId, outputPath, profile, durationSec, DateTime.Now);
    }

    public async Task StopAsync(TraceRun trace, CancellationToken cancellationToken)
    {
        Process process = trace.Run.Process;
        bool wasKilled = false;

        // С --duration dotnet-trace завершится сам после сбора.
        // Дожидаемся с таймаутом (длительность + запас) на случай подвисания.
        _ui.Info("Ожидание завершения dotnet-trace (--duration) ...");

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(trace.DurationSec + GracefulStopTimeoutSec));
            await process.WaitForExitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            wasKilled = true;
            _ui.Warn("dotnet-trace не завершился в отведённое время — принудительное завершение (taskkill /F).");
            await Task.Run(() => TaskKill(process.Id, force: true), cancellationToken);
            await process.WaitForExitAsync(cancellationToken);
        }

        string stdout = await trace.Run.StdOutTask;
        string stderr = await trace.Run.StdErrTask;

        if (wasKilled)
        {
            _ui.Warn($"dotnet-trace принудительно завершён (ExitCode: {process.ExitCode}).");
        }
        else
        {
            _ui.Ok($"dotnet-trace завершился штатно (ExitCode: {process.ExitCode}).");
        }

        if (!File.Exists(trace.OutputPath))
        {
            _ui.Warn("Файл .nettrace не создан. Вывод инструмента:");
            if (!string.IsNullOrWhiteSpace(stdout))
            {
                _ui.Detail(stdout);
            }

            if (!string.IsNullOrWhiteSpace(stderr))
            {
                _ui.Detail(stderr);
            }
        }
        else
        {
            long size = new FileInfo(trace.OutputPath).Length;
            _ui.Ok($"Trace создан: {trace.OutputPath} ({size:N0} байт).");
        }
    }

    /// <summary>Формирует аргументы dotnet-trace в зависимости от профиля (с --duration).</summary>
    private static string BuildArgs(int processId, string outputPath, string profile, int durationSec)
    {
        string providers = profile switch
        {
            "cpu-sampling" => "--profile cpu-sampling",
            "contention" => "--providers Microsoft-Windows-DotNETRuntime:0x4000:5",
            "contention-cpu" =>
                "--providers Microsoft-Windows-DotNETRuntime:0x4000:5,Microsoft-DotNETCore-SampleProfiler:0:5",
            _ => "--profile gc-verbose",
        };

        // В dotnet-trace v8+ --duration принимает TimeSpan в формате dd:hh:mm:ss,
        // а не целое число секунд (как было в v6/v7). Передаём явный формат.
        // Пример: 90с → 00:01:30 (dd:hh:mm:ss).
        string duration = FormatDuration(durationSec);

        return $"collect --process-id {processId} --output \"{outputPath}\" {providers} --duration {duration}";
    }

    /// <summary>Форматирует секунды в формат dd:hh:mm:ss для dotnet-trace v8+.</summary>
    private static string FormatDuration(int totalSeconds)
    {
        int days = totalSeconds / 86400;
        int hours = (totalSeconds % 86400) / 3600;
        int minutes = (totalSeconds % 3600) / 60;
        int seconds = totalSeconds % 60;

        return $"{days}:{hours:D2}:{minutes:D2}:{seconds:D2}";
    }

    private static void TaskKill(int processId, bool force)
    {
        try
        {
            string args = force ? $"/PID {processId} /F" : $"/PID {processId}";
            Process.Start(new ProcessStartInfo
            {
                FileName = "taskkill",
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
        }
        catch (Exception ex)
        {
            // Не блокируем основной поток при сбое taskkill.
            System.Console.Error.WriteLine($"taskkill не выполнен: {ex.Message}");
        }
    }
}
