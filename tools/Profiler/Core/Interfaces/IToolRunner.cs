namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Запуск внешнего инструмента с перенаправлением вывода и асинхронным чтением.</summary>
public interface IToolRunner
{
    /// <summary>Запускает <c>exe</c> с аргументами, не открывая новое окно, и возвращает запущенный процесс.</summary>
    Task<ToolRun> RunAsync(string exe, string args, CancellationToken cancellationToken);
}
