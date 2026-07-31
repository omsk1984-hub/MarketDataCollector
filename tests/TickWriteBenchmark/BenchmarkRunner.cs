using MarketDataCollector.Domain.Entities;
using Npgsql;
using NpgsqlTypes;

namespace TickWriteBenchmark;

public sealed class BenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly TickDataGenerator _generator;
    private readonly TableCleaner _cleaner;

    private static readonly (string Name, Func<string, List<RawTick>, Task<int>> WriteAsync)[] Methods =
    [
        ("BinaryCopyDirect", BinaryCopyDirectChunk),
        ("BulkInsertFastAsync", BulkInsertFastAsyncChunk),
        ("BulkInsertIgnoreConflicts", BulkInsertIgnoreConflictsChunk)
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
        var allTicks = _generator.Generate(totalTicks, _config.Tickers, _config.Exchange);
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
        var allTicks = _generator.Generate(totalTicks, _config.Tickers, _config.Exchange);
        var chunks = _generator.SplitIntoChunks(allTicks, chunkSize);

        // Ограничиваем параллелизм, чтобы не превысить max_connections в PostgreSQL.
        // По умолчанию max_connections=100, оставляем запас для других соединений.
        var maxParallelism = Math.Min(chunks.Count, Environment.ProcessorCount * 2);
        maxParallelism = Math.Min(maxParallelism, 20); //afety cap

        var sw = System.Diagnostics.Stopwatch.StartNew();

        var options = new ParallelOptions { MaxDegreeOfParallelism = maxParallelism };
        await Parallel.ForEachAsync(chunks, options, async (chunk, ct) =>
        {
            await writeAsync(connStr, chunk);
        });
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
            writer.Write(t.Id, NpgsqlDbType.Uuid);
            writer.Write(t.Ticker, NpgsqlDbType.Varchar);
            writer.Write(t.Price, NpgsqlDbType.Numeric);
            writer.Write(t.Volume, NpgsqlDbType.Numeric);
            writer.Write(t.Timestamp, NpgsqlDbType.TimestampTz);
            writer.Write(t.Exchange, NpgsqlDbType.Varchar);
            writer.Write(t.ReceivedAt, NpgsqlDbType.TimestampTz);
            writer.Write(t.Normalized, NpgsqlDbType.Boolean);
        }

        await writer.CompleteAsync();
        return chunk.Count;
    }

    /// <summary>
    /// Production-путь: Binary COPY во временную таблицу + INSERT...ON CONFLICT DO NOTHING.
    /// Воспроизводит логику RawTickRepository.BulkInsertFastAsync().
    ///
    /// Шаги:
    /// 1. CREATE TEMP TABLE rawticks_staging (DROP + CREATE для чистого стейта)
    /// 2. Binary COPY в temp table
    /// 3. INSERT INTO rawticks ... ON CONFLICT DO NOTHING из temp table
    ///
    /// ПЛЮС: обработка дубликатов без ошибок.
    /// МИНУС: overhead от создания temp table и INSERT INTO...SELECT.
    /// </summary>
    private static async Task<int> BulkInsertFastAsyncChunk(string connStr, List<RawTick> chunk)
    {
        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        // 1. Пересоздаём временную таблицу
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                DROP TABLE IF EXISTS rawticks_staging;
                CREATE TEMP TABLE rawticks_staging (
                    id UUID,
                    ticker VARCHAR(20),
                    price DECIMAL(18,8),
                    volume DECIMAL(18,8),
                    timestamp TIMESTAMPTZ,
                    exchange VARCHAR(50),
                    receivedat TIMESTAMPTZ,
                    normalized BOOLEAN
                );";
            await cmd.ExecuteNonQueryAsync();
        }

        // 2. Binary COPY во временную таблицу
        await using (var writer = conn.BeginBinaryImport(
            "COPY rawticks_staging (id, ticker, price, volume, timestamp, exchange, receivedat, normalized) FROM STDIN (FORMAT BINARY)"))
        {
            for (int i = 0; i < chunk.Count; i++)
            {
                var t = chunk[i];
                writer.StartRow();
                writer.Write(t.Id, NpgsqlDbType.Uuid);
                writer.Write(t.Ticker, NpgsqlDbType.Varchar);
                writer.Write(t.Price, NpgsqlDbType.Numeric);
                writer.Write(t.Volume, NpgsqlDbType.Numeric);
                writer.Write(t.Timestamp, NpgsqlDbType.TimestampTz);
                writer.Write(t.Exchange, NpgsqlDbType.Varchar);
                writer.Write(t.ReceivedAt, NpgsqlDbType.TimestampTz);
                writer.Write(t.Normalized, NpgsqlDbType.Boolean);
            }
            await writer.CompleteAsync();
        }

        // 3. INSERT INTO rawticks ... ON CONFLICT DO NOTHING из временной таблицы
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                WITH inserted AS (
                    INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized)
                    SELECT id, ticker, price, volume, timestamp, exchange, receivedat, normalized
                    FROM rawticks_staging
                    ON CONFLICT (ticker, exchange, timestamp) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*) FROM inserted;";
            cmd.CommandTimeout = 120;
            var result = await cmd.ExecuteScalarAsync();
            return result is int i ? i : Convert.ToInt32(result);
        }
    }

    /// <summary>
    /// Массовый INSERT через параметризованные VALUES + ON CONFLICT DO NOTHING.
    /// Воспроизводит логику RawTickRepository.BulkInsertIgnoreConflictsAsync().
    ///
    /// ПЛЮС: нет temp table, один SQL-запрос.
    /// МИНУС: парсинг SQL на стороне PostgreSQL, большой пакет параметров.
    /// </summary>
    private static async Task<int> BulkInsertIgnoreConflictsChunk(string connStr, List<RawTick> chunk)
    {
        if (chunk.Count == 0)
            return 0;

        await using var conn = new NpgsqlConnection(connStr);
        await conn.OpenAsync();

        const string sql = @"
            INSERT INTO rawticks (""id"", ""ticker"", ""price"", ""volume"", ""timestamp"", ""exchange"", ""receivedat"", ""normalized"")
            VALUES {0}
            ON CONFLICT (""ticker"", ""exchange"", ""timestamp"") DO NOTHING;";

        var parameters = new List<NpgsqlParameter>();
        var valueRows = new List<string>();

        for (int i = 0; i < chunk.Count; i++)
        {
            var entity = chunk[i];
            parameters.AddRange(new[]
            {
                new NpgsqlParameter($"@p{i}_id", NpgsqlDbType.Uuid) { Value = entity.Id },
                new NpgsqlParameter($"@p{i}_ticker", NpgsqlDbType.Varchar, 20) { Value = entity.Ticker },
                new NpgsqlParameter($"@p{i}_price", NpgsqlDbType.Numeric) { Value = entity.Price },
                new NpgsqlParameter($"@p{i}_volume", NpgsqlDbType.Numeric) { Value = entity.Volume },
                new NpgsqlParameter($"@p{i}_timestamp", NpgsqlDbType.TimestampTz) { Value = entity.Timestamp },
                new NpgsqlParameter($"@p{i}_exchange", NpgsqlDbType.Varchar, 50) { Value = entity.Exchange },
                new NpgsqlParameter($"@p{i}_receivedat", NpgsqlDbType.TimestampTz) { Value = entity.ReceivedAt },
                new NpgsqlParameter($"@p{i}_normalized", NpgsqlDbType.Boolean) { Value = entity.Normalized }
            });

            valueRows.Add($"(@p{i}_id, @p{i}_ticker, @p{i}_price, @p{i}_volume, @p{i}_timestamp, @p{i}_exchange, @p{i}_receivedat, @p{i}_normalized)");
        }

        var formattedSql = string.Format(sql, string.Join(", ", valueRows));

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = formattedSql;
        cmd.Parameters.AddRange(parameters.ToArray());
        cmd.CommandTimeout = 120;

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        // ExecuteNonQuery возвращает количество строк, включая ON CONFLICT DO NOTHING.
        // Для точного подсчёта нужно COUNT через WITH inserted AS (...).
        // Но для бенчмарка достаточно — мы измеряем время.
        return rowsAffected;
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
