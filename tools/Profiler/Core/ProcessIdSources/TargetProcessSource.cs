using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.ProcessIdSources;

/// <summary>Источник PID по имени процесса через <see cref="Process.GetProcessesByName"/>.</summary>
public sealed class TargetProcessSource : IProcessIdSource
{
    private readonly ILogger<TargetProcessSource> _logger;
    private readonly string _processName;

    public TargetProcessSource(ILogger<TargetProcessSource> logger, string processName)
    {
        _logger = logger;
        _processName = processName;
    }

    public int Priority => 1;

    public Task<ProcessIdCandidate?> TryFindAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Process[] processes = Process.GetProcessesByName(_processName);
        try
        {
            if (processes.Length > 0)
            {
                Process proc = processes[0];
                _logger.LogDebug("TargetProcessSource нашёл процесс {Name} PID {Pid}.", _processName, proc.Id);
                return Task.FromResult<ProcessIdCandidate?>(new ProcessIdCandidate(proc.Id, _processName, Priority));
            }
        }
        finally
        {
            foreach (Process proc in processes)
            {
                proc.Dispose();
            }
        }

        return Task.FromResult<ProcessIdCandidate?>(null);
    }
}
