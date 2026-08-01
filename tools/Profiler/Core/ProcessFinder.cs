using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Координатор поиска PID процесса. Перебирает стратегии по приоритету
/// (OCP). Если ни один источник не нашёл процесс — завершает работу кодом 1.
/// </summary>
public sealed class ProcessFinder : IProcessFinder
{
    private readonly IEnumerable<IProcessIdSource> _sources;
    private readonly IConsoleUI _ui;
    private readonly ILogger<ProcessFinder> _logger;

    public ProcessFinder(
        IEnumerable<IProcessIdSource> sources,
        IConsoleUI ui,
        ILogger<ProcessFinder> logger)
    {
        _sources = sources;
        _ui = ui;
        _logger = logger;
    }

    public int FindProcessId(CancellationToken cancellationToken)
    {
        // Сортируем источники по приоритету: TracePs → TargetProcess → Wmi.
        List<IProcessIdSource> ordered = _sources.OrderBy(s => s.Priority).ToList();

        foreach (IProcessIdSource source in ordered)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _ui.Detail($"Поиск процесса через источник: {source.GetType().Name}");

            ProcessIdCandidate? candidate = source.TryFindAsync(cancellationToken).GetAwaiter().GetResult();
            if (candidate is not null)
            {
                _ui.Ok($"Найден процесс: {candidate.Name} (PID {candidate.ProcessId}).");
                return candidate.ProcessId;
            }
        }

        _ui.Error("Не удалось найти процесс Worker. Убедитесь, что он запущен.");
        _logger.LogError("Ни один источник не нашёл целевой процесс.");
        Environment.Exit(1);
        return -1; // Недостижимо, но компилятор требует возврат.
    }
}
