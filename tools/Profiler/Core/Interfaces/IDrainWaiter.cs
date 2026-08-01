namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Ожидание дренажа очередей перед вторым gcdump.</summary>
public interface IDrainWaiter
{
    /// <summary>
    /// Ожидает обнуления fill_level-очередей (по метрике processor.channel.fill_level,
    /// экспортируется как processor_channel_fill_level_count), опрашивая /metrics.
    /// При недоступности метрик — обратный отсчёт таймаута.
    /// </summary>
    Task WaitForDrainAsync(int timeoutSec, string metricsUrl, CancellationToken cancellationToken);
}
