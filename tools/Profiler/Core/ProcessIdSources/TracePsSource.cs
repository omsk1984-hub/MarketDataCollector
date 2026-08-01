using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.ProcessIdSources;

/// <summary>
/// Источник PID через <c>dotnet-trace ps</c> — таблица "PID  NAME". Самый надёжный,
/// поэтому имеет наивысший приоритет.
/// </summary>
public sealed class TracePsSource : IProcessIdSource
{
    private readonly IToolRunner _toolRunner;
    private readonly ILogger<TracePsSource> _logger;
    private readonly string _processName;

    public TracePsSource(IToolRunner toolRunner, ILogger<TracePsSource> logger, string processName)
    {
        _toolRunner = toolRunner;
        _logger = logger;
        _processName = processName;
    }

    public int Priority => 0;

    public async Task<ProcessIdCandidate?> TryFindAsync(CancellationToken cancellationToken)
    {
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", "ps", cancellationToken);
        await run.Process.WaitForExitAsync(cancellationToken);

        string stdout = await run.StdOutTask;

        foreach (string rawLine in stdout.Split('\n'))
        {
            string line = rawLine.Trim();
            if (line.Length == 0)
            {
                continue;
            }

            // Формат: "<PID>  <NAME>"
            string[] parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
            {
                continue;
            }

            if (!int.TryParse(parts[0], out int pid))
            {
                continue;
            }

            string name = parts[1];
            if (name.Equals(_processName, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("TracePsSource нашёл PID {Pid} (Name={Name}).", pid, name);
                return new ProcessIdCandidate(pid, name, Priority);
            }
        }

        return null;
    }
}
