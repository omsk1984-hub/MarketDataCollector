using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.Waiters;

/// <summary>Ожидание достижения пика нагрузки перед первым gcdump.</summary>
public sealed class PeakLoadWaiter : IPeakLoadWaiter
{
    private const int ChunkSeconds = 5;

    private readonly IConsoleUI _ui;
    private readonly ILogger<PeakLoadWaiter> _logger;

    public PeakLoadWaiter(IConsoleUI ui, ILogger<PeakLoadWaiter> logger)
    {
        _ui = ui;
        _logger = logger;
    }

    public async Task WaitForPeakLoadAsync(int seconds, CancellationToken cancellationToken)
    {
        _ui.Info($"Ожидание пика нагрузки: {seconds}с ...");

        int remaining = Math.Max(0, seconds);
        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int chunk = Math.Min(ChunkSeconds, remaining);
            await Task.Delay(TimeSpan.FromSeconds(chunk), cancellationToken);
            remaining -= chunk;

            _logger.LogDebug("До пика осталось: {Remaining}с", remaining);
        }

        _ui.Ok("Достигнут момент пика нагрузки.");
    }
}
