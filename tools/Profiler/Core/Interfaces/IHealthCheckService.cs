namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Ожидание готовности Worker по health-check endpoint.</summary>
public interface IHealthCheckService
{
    /// <summary>
    /// Ожидает HTTP 200 и статуса "healthy". При таймауте завершает процесс кодом 1.
    /// </summary>
    Task WaitForHealthyAsync(string healthUrl, int timeoutSec, CancellationToken cancellationToken);
}
