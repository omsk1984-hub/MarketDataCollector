namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Парсер Prometheus-текстового формата exposition.</summary>
public interface IPrometheusParser
{
    /// <summary>Разбирает текст метрик в коллекцию образцов.</summary>
    IReadOnlyList<MetricSample> Parse(string body);
}
