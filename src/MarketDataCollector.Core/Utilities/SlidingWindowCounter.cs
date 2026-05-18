using System.Threading;

namespace MarketDataCollector.Core.Utilities;

/// <summary>
/// Lock-free счётчик RPS со скользящим окном (sliding window).
/// Использует кольцевой буфер из 60 элементов — по одному bucket'у на секунду.
/// Все операции — через <see cref="Interlocked"/>, без блокировок и без таймеров.
/// </summary>
public sealed class SlidingWindowCounter
{
    /// <summary>
    /// Размер окна в секундах.
    /// </summary>
    public const int WindowSize = 60;

    private readonly long[] _buckets = new long[WindowSize];
    private readonly long[] _bucketTimes = new long[WindowSize];

    /// <summary>
    /// Инкрементирует счётчик для текущей секунды.
    /// Потокобезопасно, lock-free.
    /// </summary>
    public void Increment()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var idx = (int)(now % WindowSize);

        // Если bucket устарел — сбрасываем через Interlocked.Exchange
        var storedTime = Interlocked.Read(ref _bucketTimes[idx]);
        if (storedTime != now)
        {
            var prevTime = Interlocked.Exchange(ref _bucketTimes[idx], now);
            if (prevTime != now)
            {
                // Мы первый, кто обновил время — сбрасываем счётчик
                Interlocked.Exchange(ref _buckets[idx], 0);
            }
        }

        Interlocked.Increment(ref _buckets[idx]);
    }

    /// <summary>
    /// Возвращает средний RPS за последние <paramref name="lastSeconds"/> секунд.
    /// </summary>
    /// <param name="lastSeconds">Количество секунд для усреднения (по умолчанию 10, макс. 60).</param>
    /// <returns>Среднее количество вызовов в секунду.</returns>
    public double GetRps(int lastSeconds = 10)
    {
        if (lastSeconds <= 0) return 0;
        if (lastSeconds > WindowSize) lastSeconds = WindowSize;

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long total = 0;
        var count = 0;

        for (int i = 0; i < lastSeconds; i++)
        {
            var bucketTime = now - i;
            var idx = (int)(bucketTime % WindowSize);

            var storedTime = Interlocked.Read(ref _bucketTimes[idx]);
            if (storedTime == bucketTime)
            {
                total += Interlocked.Read(ref _buckets[idx]);
                count++;
            }
        }

        return count > 0 ? (double)total / count : 0;
    }

    /// <summary>
    /// Сбрасывает все счётчики в ноль.
    /// </summary>
    public void Reset()
    {
        for (int i = 0; i < WindowSize; i++)
        {
            Interlocked.Exchange(ref _bucketTimes[i], 0);
            Interlocked.Exchange(ref _buckets[i], 0);
        }
    }
}
