using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace MarketDataCollector.Core.Telemetry;

/// <summary>
/// Локальный аккумулятор для OTel <see cref="Counter{long}"/>.
///
/// Позволяет убрать вызовы <c>Counter<long>.Add</c> (внутри которых OTel
/// использует lock/Monitor в <c>AggregatorStore</c>) из per-message hot path.
///
/// Hot path (<see cref="Add"/>) — только <see cref="Interlocked.Increment"/>:
///   - zero-lock (без внутренних lock OpenTelemetry);
///   - zero-alloc (без создания <c>KeyValuePair</c> на каждый инкремент).
///
/// Один раз за интервал/батч вызывается <see cref="Flush"/> — только тогда
/// происходит реальный <c>Counter.Add</c> с предсозданными тегами.
/// </summary>
public sealed class CounterBatcher
{
    private readonly Counter<long> _counter;
    private readonly KeyValuePair<string, object?>[] _tags;
    private long _count;

    /// <summary>
    /// Создаёт аккумулятор для конкретного счётчика с фиксированным набором тегов.
    /// </summary>
    /// <param name="counter">Целевой OTel-счётчик. Не null.</param>
    /// <param name="tags">Предсозданный (обычно статический) массив тегов. Не null.</param>
    public CounterBatcher(Counter<long> counter, KeyValuePair<string, object?>[] tags)
    {
        ArgumentNullException.ThrowIfNull(counter);
        ArgumentNullException.ThrowIfNull(tags);
        _counter = counter;
        _tags = tags;
    }

    /// <summary>
    /// Инкремент счётчика в hot path: атомарно, без lock и аллокаций.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add() => Interlocked.Increment(ref _count);

    /// <summary>
    /// Текущее накопленное значение (не вынесенное в OTel).
    /// </summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>
    /// Выносит накопленное значение в <see cref="Counter{long}"/> одним вызовом <c>Add</c>.
    /// При нулевом остатке — no-op (не вызывает <c>Add(0)</c>).
    /// </summary>
    public void Flush()
    {
        long n = Interlocked.Exchange(ref _count, 0);
        if (n != 0)
            _counter.Add(n, _tags);
    }

    /// <summary>
    /// Финальный сброс без потери остатка. Аналогичен <see cref="Flush"/>.
    /// </summary>
    public void FlushAndReset() => Flush();
}
