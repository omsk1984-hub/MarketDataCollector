using MarketDataCollector.Domain.Entities;
using Npgsql;

namespace TickWriteBenchmark;

public sealed class BenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly TickDataGenerator _generator;
    private readonly TableCleaner _cleaner;

    private static readonly (string Name, Func<string, List<RawTick>, Task<int>> WriteAsync)[] Methods =
    [
        ("BinaryCopy", BinaryCopyDirectChunk)
    ];

    public BenchmarkRunner(BenchmarkConfig config, TickDataGenerator generator, TableCleaner cleaner)
    {
        _config = config;
        _generator = generator;
        _cleaner = cleaner;
    }

    public async Task<List<BenchmarkResult>> RunAllBenchmarksAsync()
    {
        var results = new List<BenchmarkResult>(Methods.Length * _config.ChunkSizes.Length * 2);

        foreach (var (name, writeAsync) in Methods)
        {
            foreach (var chunkSize in _config.ChunkSizes)
            {
                // Очистка БД и задержка перед sequential тестом
                await _cleaner.TruncateAsync();
                await Task.Delay(2000);

                Console.WriteLine();
                Console.WriteLine($"=== {name} | chunk={chunkSize} | sequential ===");
                var seqResult = await RunSequentialAsync(name, chunkSize, writeAsync);
                results.Add(seqResult);
                Console.WriteLine($"  Time: {seqResult.ElapsedMs,8:F1} ms  |  {seqResult.TicksPerSec,8:F0} ticks/sec");

                // Очистка БД и задержка перед parallel тестом
                await _cleaner.TruncateAsync();
                await Task.Delay(2000);

                Console.WriteLine($"=== {name} | chunk={chunkSize} | parallel ===");
                var parResult = await RunParallelAsync(name, chunkSize, writeAsync);
                results.Add(parResult);
                Console.WriteLine($"  Time: {parResult.ElapsedMs,8:F1} ms  |  {parResult.TicksPerSec,8:F0} ticks/sec");
            }
        }

        return results;
    }

    private async Task<BenchmarkResult> RunSequentialAsync(
        string methodName,
        int chunkSize,
        Func<string, List<RawTick>, Task<int>> writeAsync)
    {
        var totalTicks = _config.TotalTicks;
        var connStr = _config.ConnectionString;
        var allTicks = _generator.Generate(totalTicks);
        var chunks = _generator.SplitIntoChunks(allTicks, chunkSize);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        foreach (var chunk in chunks)
            await writeAsync(connStr, chunk);

        sw.Stop();

        return NewResult(methodName, "Sequential", chunkSize, totalTicks, chunks.Count, sw);
    }

    private async Task<BenchmarkResult> RunParallelAsync(
        string methodName,
        int chunkSize,
        Func<string, List<RawTick>, Task<int>> writeAsync)
    {
        var totalTicks = _config.TotalTicks;
        var connStr = _config.ConnectionString;
        var allTicks = _generator.Generate(totalTicks);
        var chunks = _generator.SplitIntoChunks(allTicks, chunkSize);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        var tasks = chunks.Select(chunk => Task.Run(() => writeAsync(connStr, chunk)));
        await Task.WhenAll(tasks);
        sw.Stop();

        return NewResult(methodName, "Parallel", chunkSize, totalTicks, chunks.Count, sw);
    }

    /// <summary>
    /// Прямой Binary COPY в таблицу rawticks.
    /// Самый быстрый способ вставки — бинарный протокол PostgreSQL, без парсинга SQL.
    /// https://www.npgsql.org/doc/copy.html
    ///
    /// МИНУС: не поддерживает ON CONFLICT. Если дубликат — будет ошибка.
    /// В тесте все данные уникальны, поэтому это допустимо.
    /// </summary>
    private static async Task<int> BinaryCopyDirectChunk(string connStr, List<RawTick> chunk)
    {
        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        await using var writer = conn.BeginBinaryImport(
            "COPY rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized) FROM STDIN (FORMAT BINARY)");

        for (int i = 0; i < chunk.Count; i++)
        {
            var t = chunk[i];
            writer.StartRow();
            writer.Write(t.Id, NpgsqlTypes.NpgsqlDbType.Uuid);
            writer.Write(t.Ticker, NpgsqlTypes.NpgsqlDbType.Varchar);
            writer.Write(t.Price, NpgsqlTypes.NpgsqlDbType.Numeric);
            writer.Write(t.Volume, NpgsqlTypes.NpgsqlDbType.Numeric);
            writer.Write(t.Timestamp, NpgsqlTypes.NpgsqlDbType.TimestampTz);
            writer.Write(t.Exchange, NpgsqlTypes.NpgsqlDbType.Varchar);
            writer.Write(t.ReceivedAt, NpgsqlTypes.NpgsqlDbType.TimestampTz);
            writer.Write(t.Normalized, NpgsqlTypes.NpgsqlDbType.Boolean);
        }

        await writer.CompleteAsync();
        return chunk.Count;
    }

    private static BenchmarkResult NewResult(
        string method, string mode, int chunkSize,
        int totalTicks, int chunksCount,
        System.Diagnostics.Stopwatch sw) => new(
            Method: method,
            Mode: mode,
            ChunkSize: chunkSize,
            TotalTicks: totalTicks,
            ChunksCount: chunksCount,
            ElapsedMs: sw.Elapsed.TotalMilliseconds,
            TicksPerSec: totalTicks / sw.Elapsed.TotalSeconds);
}
