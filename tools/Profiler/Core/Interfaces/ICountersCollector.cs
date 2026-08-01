namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Фоновый сбор Prometheus-метрик в CSV.</summary>
public interface ICountersCollector
{
    /// <summary>Запускает фоновый сбор до отмены токена.</summary>
    Task StartAsync(string outputCsvPath, CancellationToken cancellationToken);
}
