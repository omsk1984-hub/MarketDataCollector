using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Services;

namespace TickWriteBenchmark;

public sealed class TickDataGenerator
{
    private static readonly SystemTimeService TimeService = new();

    /// <summary>
    /// Генерирует указанное количество тиков с уникальными timestamp.
    /// Тикеры ротируются из переданного списка (как в реальности — каждый consumer пишет свой тикер).
    /// </summary>
    public List<RawTick> Generate(int count, string[]? tickers = null, string exchange = "BENCH")
    {
        var tickerArray = tickers ?? ["BENCHTEST"];
        var ticks = new List<RawTick>(count);
        var baseTimestamp = DateTime.UtcNow;

        for (int i = 0; i < count; i++)
        {
            // Каждый тик имеет уникальный timestamp с шагом 1 мс
            // (PostgreSQL TIMESTAMPTZ хранит микросекунды, шаг 100 нс = 1 tick приводит к коллизиям)
            var tickTimestamp = baseTimestamp.AddMilliseconds(i);

            // Ротация тикеров — имитирует реальную нагрузку с несколькими символами
            var ticker = tickerArray[i % tickerArray.Length];

            // Базовая цена зависит от тикера (имитация разных ценовых уровней)
            var basePrice = ticker switch
            {
                "BTCUSDT" => 50000.00m,
                "ETHUSDT" => 3000.00m,
                "SOLUSDT" => 150.00m,
                _ => 50000.00m
            };

            var tick = new RawTick(
                ticker: ticker,
                price: basePrice + (i % 100),
                volume: 0.1m + (i % 10) * 0.01m,
                timestamp: tickTimestamp,
                exchange: exchange,
                timeService: TimeService);

            ticks.Add(tick);
        }

        return ticks;
    }

    /// <summary>
    /// Генерирует указанное количество тиков с уникальными timestamp (обратная совместимость).
    /// </summary>
    public List<RawTick> Generate(int count)
    {
        return Generate(count, ["BENCHTEST"], "BENCH");
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
