using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Запуск внешнего инструмента без открытия нового окна, с перенаправлением вывода
/// и асинхронным чтением stdout/stderr (избегает взаимоблокировки pipe-буфера).
/// </summary>
public sealed class ToolRunner : IToolRunner
{
    private readonly ILogger<ToolRunner> _logger;

    public ToolRunner(ILogger<ToolRunner> logger)
    {
        _logger = logger;
    }

    public Task<ToolRun> RunAsync(string exe, string args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(exe);
        ArgumentNullException.ThrowIfNull(args);

        _logger.LogDebug("Запуск инструмента: {Exe} {Args}", exe, args);

        ProcessStartInfo startInfo = new()
        {
            FileName = exe,
            Arguments = args,
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardOutputEncoding = System.Text.Encoding.UTF8,
            StandardErrorEncoding = System.Text.Encoding.UTF8,
        };

        Process process = new() { StartInfo = startInfo };
        if (!process.Start())
        {
            throw new InvalidOperationException($"Не удалось запустить процесс: {exe}");
        }

        // Асинхронное чтение потоков, чтобы избежать deadlock из-за заполнения буфера.
        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        Task<string> stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);

        return Task.FromResult(new ToolRun(process, string.Empty, stdoutTask, stderrTask));
    }
}
