using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Сбор дампа управляемой кучи через dotnet-gcdump.</summary>
public sealed class GcDumpCollector : IGcDumpCollector
{
    private readonly IToolRunner _toolRunner;
    private readonly IConsoleUI _ui;
    private readonly ILogger<GcDumpCollector> _logger;

    public GcDumpCollector(IToolRunner toolRunner, IConsoleUI ui, ILogger<GcDumpCollector> logger)
    {
        _toolRunner = toolRunner;
        _ui = ui;
        _logger = logger;
    }

    public async Task<GcDumpResult> CollectAsync(
        int processId,
        string outputPath,
        string label,
        CancellationToken cancellationToken)
    {
        if (!ProcessIsAlive(processId))
        {
            _ui.Warn($"Процесс PID {processId} не запущен — gcdump ({label}) пропущен.");
            return new GcDumpResult(outputPath, 0, -1);
        }

        _ui.Info($"Сбор gcdump ({label}) PID {processId} -> {outputPath} ...");

        string args = $"collect --process-id {processId} --output \"{outputPath}\"";
        ToolRun run = await _toolRunner.RunAsync("dotnet-gcdump", args, cancellationToken);

        // Ожидание завершения до 120 секунд.
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(120));
            await run.Process.WaitForExitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _ui.Warn($"dotnet-gcdump не завершился за 120с — принудительное завершение (PID {run.Process.Id}).");
            await Task.Run(() => KillByProcessId(run.Process.Id), cancellationToken);
        }

        string stdout = await run.StdOutTask;
        string stderr = await run.StdErrTask;

        _logger.LogDebug("gcdump ExitCode={ExitCode}; stdout len={StdLen}; stderr len={ErrLen}.",
            run.Process.ExitCode, stdout.Length, stderr.Length);

        long fileSize = File.Exists(outputPath) ? new FileInfo(outputPath).Length : 0;

        if (fileSize > 0)
        {
            _ui.Ok($"gcdump ({label}) создан: {outputPath} ({fileSize:N0} байт).");
        }
        else
        {
            _ui.Warn($"gcdump ({label}) не создан. Код выхода: {run.Process.ExitCode}.");
        }

        return new GcDumpResult(outputPath, fileSize, run.Process.ExitCode);
    }

    private static bool ProcessIsAlive(int processId)
    {
        try
        {
            using Process? proc = Process.GetProcessById(processId);
            return proc is not null && !proc.HasExited;
        }
        catch (ArgumentException)
        {
            return false;
        }
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
            System.Console.Error.WriteLine($"taskkill не выполнен: {ex.Message}");
        }
    }
}
