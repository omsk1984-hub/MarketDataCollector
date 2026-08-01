namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Ожидание дренажа очередей перед вторым gcdump.</summary>
public interface IDrainWaiter
{
    /// <summary>
    /// Ожидает обнуления backlog-очередей, опрашивая /metrics.
    /// При недоступности метрик — обратный отсчёт таймаута.
    /// </summary>
    Task WaitForDrainAsync(int timeoutSec, string metricsUrl, CancellationToken cancellationToken);
}
