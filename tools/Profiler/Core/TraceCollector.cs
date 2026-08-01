using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Управление сбором trace через dotnet-trace.</summary>
public sealed class TraceCollector : ITraceCollector
{
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
        string args = BuildArgs(processId, durationSec, outputPath, profile);

        _ui.Info($"Запуск dotnet-trace (PID {processId}, {durationSec}с, профиль {profile}) ...");
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", args, cancellationToken);

        return new TraceRun(run, processId, outputPath, profile);
    }

    public async Task StopAsync(TraceRun trace, CancellationToken cancellationToken)
    {
        Process process = trace.Run.Process;

        _ui.Info("Ожидание завершения dotnet-trace ...");

        // Ждём до 15 секунд завершения процесса.
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(15));
            await process.WaitForExitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _ui.Warn("dotnet-trace не завершился за 15с — принудительное завершение (taskkill).");
            await Task.Run(() => KillByProcessId(process.Id), cancellationToken);
        }

        string stdout = await trace.Run.StdOutTask;
        string stderr = await trace.Run.StdErrTask;

        _ui.Detail($"dotnet-trace ExitCode: {process.ExitCode}");

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

    /// <summary>Формирует аргументы dotnet-trace в зависимости от профиля.</summary>
    private static string BuildArgs(int processId, int durationSec, string outputPath, string profile)
    {
        // ВАЖНО: dotnet-trace интерпретирует голое число как дни. Формат обязателен hh:mm:ss.
        TimeSpan duration = TimeSpan.FromSeconds(Math.Max(1, durationSec));
        string durationArg = $"{(int)duration.TotalHours:00}:{duration.Minutes:00}:{duration.Seconds:00}";

        string providers = profile switch
        {
            "cpu-sampling" => "--profile cpu-sampling",
            "contention" => "--providers Microsoft-Windows-DotNETRuntime:0x4000:5",
            "contention-cpu" =>
                "--providers Microsoft-Windows-DotNETRuntime:0x4000:5,Microsoft-DotNETCore-SampleProfiler:0:5",
            _ => "--profile gc-verbose",
        };

        return $"collect --process-id {processId} --output \"{outputPath}\" --duration {durationArg} {providers}";
    }

    private static void KillByProcessId(int processId)
    {
        try
        {
            Process.Start(new ProcessStartInfo
            {
                FileName = "taskkill",
                Arguments = $"/PID {processId} /F",
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
