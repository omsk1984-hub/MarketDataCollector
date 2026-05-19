using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Services;

namespace TickWriteBenchmark;

public sealed class TickDataGenerator
{
    private static readonly SystemTimeService TimeService = new();

    /// <summary>
    /// Генерирует указанное количество тиков с уникальными timestamp.
    /// </summary>
    public List<RawTick> Generate(int count)
    {
        var ticks = new List<RawTick>(count);
        var baseTimestamp = DateTime.UtcNow;

        for (int i = 0; i < count; i++)
        {
            // Каждый тик имеет уникальный timestamp с шагом 1 мс
            // (PostgreSQL TIMESTAMPTZ хранит микросекунды, шаг 100 нс = 1 tick приводит к коллизиям)
            var tickTimestamp = baseTimestamp.AddMilliseconds(i);

            var tick = new RawTick(
                ticker: "BENCHTEST",
                price: 50000.00m + (i % 100),
                volume: 0.1m + (i % 10) * 0.01m,
                timestamp: tickTimestamp,
                exchange: "BENCH",
                timeService: TimeService);

            ticks.Add(tick);
        }

        return ticks;
    }

    /// <summary>
    /// Разбивает список тиков на чанки указанного размера.
    /// </summary>
    public List<List<RawTick>> SplitIntoChunks(List<RawTick> ticks, int chunkSize)
    {
        var chunks = new List<List<RawTick>>();
        for (int i = 0; i < ticks.Count; i += chunkSize)
        {
            chunks.Add(ticks.GetRange(i, Math.Min(chunkSize, ticks.Count - i)));
        }
        return chunks;
    }
}
