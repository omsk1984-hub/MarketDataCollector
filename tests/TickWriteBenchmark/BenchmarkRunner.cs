using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Infrastructure.Services;
using Npgsql;

namespace TickWriteBenchmark;

public sealed class BenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly TickDataGenerator _generator;
    private readonly TableCleaner _cleaner;

    private static readonly (string Name, Func<string, List<RawTick>, Task<int>> WriteAsync)[] Methods =
    [
        ("RawSqlInsert",  RawSqlInsertChunk),
        ("BinaryCopy",    BinaryCopyDirectChunk)
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
                Console.WriteLine();
                Console.WriteLine($"=== {name} | chunk={chunkSize} | sequential ===");
                var seqResult = await RunSequentialAsync(name, chunkSize, writeAsync);
                results.Add(seqResult);
                Console.WriteLine($"  Time: {seqResult.ElapsedMs,8:F1} ms  |  {seqResult.TicksPerSec,8:F0} ticks/sec");
                await _cleaner.TruncateAsync();

                Console.WriteLine($"=== {name} | chunk={chunkSize} | parallel ===");
                var parResult = await RunParallelAsync(name, chunkSize, writeAsync);
                results.Add(parResult);
                Console.WriteLine($"  Time: {parResult.ElapsedMs,8:F1} ms  |  {parResult.TicksPerSec,8:F0} ticks/sec");
                await _cleaner.TruncateAsync();
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
    /// Многострочный INSERT через сырой Npgsql (один SQL-запрос на чанк).
    /// https://www.npgsql.org/doc/basic-usage.html
    /// </summary>
    private static async Task<int> RawSqlInsertChunk(string connStr, List<RawTick> chunk)
    {
        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        var values = new List<string>(chunk.Count);
        var parameters = new List<NpgsqlParameter>(chunk.Count * 8);

        for (int i = 0; i < chunk.Count; i++)
        {
            var t = chunk[i];
            int p = i * 8;
            parameters.AddRange([
                new NpgsqlParameter($"@p{p}_id", NpgsqlTypes.NpgsqlDbType.Uuid) { Value = t.Id },
                new NpgsqlParameter($"@p{p}_ticker", NpgsqlTypes.NpgsqlDbType.Varchar, 20) { Value = t.Ticker },
                new NpgsqlParameter($"@p{p}_price", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = t.Price },
                new NpgsqlParameter($"@p{p}_volume", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = t.Volume },
                new NpgsqlParameter($"@p{p}_timestamp", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = t.Timestamp },
                new NpgsqlParameter($"@p{p}_exchange", NpgsqlTypes.NpgsqlDbType.Varchar, 50) { Value = t.Exchange },
                new NpgsqlParameter($"@p{p}_receivedat", NpgsqlTypes.NpgsqlDbType.TimestampTz) { Value = t.ReceivedAt },
                new NpgsqlParameter($"@p{p}_normalized", NpgsqlTypes.NpgsqlDbType.Boolean) { Value = t.Normalized }
            ]);

            values.Add(
                $"(@p{p}_id, @p{p}_ticker, @p{p}_price, @p{p}_volume, @p{p}_timestamp, @p{p}_exchange, @p{p}_receivedat, @p{p}_normalized)");
        }

        var sql = $"INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized) " +
                  $"VALUES {string.Join(", ", values)} " +
                  $"ON CONFLICT (ticker, exchange, timestamp) DO NOTHING;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters.ToArray());
        return await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Прямой Binary COPY в основную таблицу (без temp table).
    /// Самый быстрый способ вставки — на уровне СУБД нет никакого парсинга SQL.
    /// https://www.npgsql.org/doc/copy.html
    ///
    /// МИНУС: не поддерживает ON CONFLICT — если дубликат, будет ошибка.
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
