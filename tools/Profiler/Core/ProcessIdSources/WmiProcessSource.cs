using System.Management;
using System.Runtime.Versioning;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.ProcessIdSources;

/// <summary>
/// Источник PID через WMI (только Windows). Ищет процессы <c>dotnet.exe</c>,
/// чья командная строка содержит имя Worker, либо процесс с именем Worker.
/// Запасной вариант для окружений, где <c>dotnet-trace ps</c> не показывает целевой процесс.
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class WmiProcessSource : IProcessIdSource
{
    private readonly ILogger<WmiProcessSource> _logger;
    private readonly string _processName;

    public WmiProcessSource(ILogger<WmiProcessSource> logger, string processName)
    {
        _logger = logger;
        _processName = processName;
    }

    public int Priority => 2;

    public Task<ProcessIdCandidate?> TryFindAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            string query =
                "SELECT ProcessId, Name, CommandLine FROM Win32_Process " +
                "WHERE (Name = 'dotnet.exe' OR Name = 'MarketDataCollector.Worker.exe' OR Name = 'MarketDataCollector.Worker')";

            using var searcher = new ManagementObjectSearcher(query);
            foreach (ManagementBaseObject obj in searcher.Get())
            {
                cancellationToken.ThrowIfCancellationRequested();

                string name = obj["Name"]?.ToString() ?? string.Empty;
                string commandLine = obj["CommandLine"]?.ToString() ?? string.Empty;
                int pid = Convert.ToInt32(obj["ProcessId"]);

                if (name.Contains(_processName, StringComparison.OrdinalIgnoreCase) ||
                    commandLine.Contains(_processName, StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogDebug("WmiProcessSource нашёл PID {Pid} (Name={Name}).", pid, name);
                    return Task.FromResult<ProcessIdCandidate?>(new ProcessIdCandidate(pid, name, Priority));
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WMI-запрос не выполнен (источник пропущен): {Message}", ex.Message);
        }

        return Task.FromResult<ProcessIdCandidate?>(null);
    }
}
