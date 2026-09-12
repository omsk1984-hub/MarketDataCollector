namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>
/// Генерация topN-отчёта по trace через <c>dotnet-trace report topN</c>.
/// Выполняет пост-анализ CPU-стеков и сохраняет результат в <c>*_topn.md</c>.
/// </summary>
public interface ITopNReporter
{
    /// <summary>
    /// Запускает <c>dotnet-trace report topN</c> по trace и записывает вывод в
    /// <c><traceBase>_topn.md</c>. Возвращает путь к созданному отчёту
    /// (пустая строка, если отчёт не удалось создать).
    /// </summary>
    Task<string> GenerateAsync(string traceFile, CancellationToken cancellationToken);
}