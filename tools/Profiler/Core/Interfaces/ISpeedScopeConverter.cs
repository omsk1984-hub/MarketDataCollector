namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Конвертация .nettrace в SpeedScope-формат.</summary>
public interface ISpeedScopeConverter
{
    /// <summary>
    /// Конвертирует trace-файл в speedscope. Возвращает путь к результату
    /// (может отличаться суффиксом при дублировании имени).
    /// </summary>
    Task<string> ConvertAsync(string traceFile, CancellationToken cancellationToken);
}
