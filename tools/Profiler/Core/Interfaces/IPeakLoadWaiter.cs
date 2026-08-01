namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Ожидание достижения пика нагрузки перед первым gcdump.</summary>
public interface IPeakLoadWaiter
{
    /// <summary>Ждёт заданное число секунд (кусками по 5с) до пика нагрузки.</summary>
    Task WaitForPeakLoadAsync(int seconds, CancellationToken cancellationToken);
}
