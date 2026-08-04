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
        string args = BuildArgs(processId, outputPath, profile);

        _ui.Info($"Запуск dotnet-trace (PID {processId}, до {durationSec}с, профиль {profile}) ...");
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", args, cancellationToken);

        return new TraceRun(run, processId, outputPath, profile, durationSec, DateTime.Now);
    }

    public async Task StopAsync(TraceRun trace, CancellationToken cancellationToken)
    {
        Process process = trace.Run.Process;

        // Дожидаемся полной запрошенной длительности сбора (trace запущен без --duration,
        // поэтому остановкой управляем полностью сами — это даёт полное окно данных).
        int elapsedSec = (int)(DateTime.Now - trace.StartedAt).TotalSeconds;
        int remainingSec = Math.Max(0, trace.DurationSec - elapsedSec);
        if (remainingSec > 0)
        {
            _ui.Info($"Ожидание завершения trace: ещё ~{remainingSec}с ...");
            await Task.Delay(TimeSpan.FromSeconds(remainingSec), cancellationToken);
        }

        _ui.Info("Остановка dotnet-trace (graceful) ...");

        // Сначала мягкое завершение (без /F): dotnet-trace корректно финализирует файл
        // и завершается с кодом 0. Используем как graceful-остановку.
        await Task.Run(() => TaskKill(process.Id, force: false), cancellationToken);

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(GracefulStopTimeoutSec));
            await process.WaitForExitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _ui.Warn("dotnet-trace не завершился после graceful-остановки — принудительное завершение (taskkill /F).");
            await Task.Run(() => TaskKill(process.Id, force: true), cancellationToken);
            await process.WaitForExitAsync(cancellationToken);
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

    /// <summary>Формирует аргументы dotnet-trace в зависимости от профиля (без --duration).</summary>
    private static string BuildArgs(int processId, string outputPath, string profile)
    {
        string providers = profile switch
        {
            "cpu-sampling" => "--profile cpu-sampling",
            "contention" => "--providers Microsoft-Windows-DotNETRuntime:0x4000:5",
            "contention-cpu" =>
                "--providers Microsoft-Windows-DotNETRuntime:0x4000:5,Microsoft-DotNETCore-SampleProfiler:0:5",
            _ => "--profile gc-verbose",
        };

        return $"collect --process-id {processId} --output \"{outputPath}\" {providers}";
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
