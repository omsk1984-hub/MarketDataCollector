using Npgsql;

namespace TickWriteBenchmark;

/// <summary>
/// READ-бенчмарк: сравнение производительности SELECT-запросов
/// на обычной таблице vs партиционированной (partition pruning).
///
/// Метрики:
/// - Время выполнения запроса (среднее по N итерациям)
/// - EXPLAIN ANALYZE — план запроса (Seq Scan vs Index Scan vs Partition Pruning)
/// - Rows returned
/// </summary>
public sealed class ReadBenchmarkRunner
{
    private readonly BenchmarkConfig _config;
    private readonly TickDataGenerator _generator;
    private readonly TableCleaner _cleaner;

    public ReadBenchmarkRunner(BenchmarkConfig config, TickDataGenerator generator, TableCleaner cleaner)
    {
        _config = config;
        _generator = generator;
        _cleaner = cleaner;
    }

    /// <summary>
    /// Заполняет таблицу тестовыми данными для READ-бенчмарка.
    /// </summary>
    public async Task SeedDataAsync(string tableName = "rawticks")
    {
        Console.WriteLine($"  Seeding {tableName} with {_config.TotalTicks} ticks...");

        var ticks = _generator.Generate(_config.TotalTicks, _config.Tickers, _config.Exchange);
        var chunks = _generator.SplitIntoChunks(ticks, 5000);

        await using var conn = new NpgsqlConnection(_config.ConnectionString);
        await conn.OpenAsync();

        foreach (var chunk in chunks)
        {
            await using var writer = conn.BeginBinaryImport(
                $"COPY {tableName} (id, ticker, price, volume, timestamp, exchange, receivedat, normalized) FROM STDIN (FORMAT BINARY)");

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
        }

        Console.WriteLine($"  Seeding complete.");
    }

    /// <summary>
    /// Запускает все READ-тесты на указанной таблице.
    /// </summary>
    public async Task<List<ReadBenchmarkResult>> RunReadBenchmarksAsync(string tableName, string label)
    {
        var results = new List<ReadBenchmarkResult>();

        Console.WriteLine();
        Console.WriteLine($"=== READ Benchmarks on {tableName} ({label}) ===");

        // Тест 1: Full scan по тикеру (без фильтра по времени)
        results.Add(await RunSingleReadTestAsync(
            tableName, "FullScan", "SELECT * FROM {0} WHERE ticker = @ticker",
            GetTickerForTest(), label));

        // Тест 2: Time-range query (1 день) — должен использовать partition pruning
        var (from, to) = GetTimeRangeForTest();
        results.Add(await RunSingleReadTestAsync(
            tableName, "TimeRange_1day", "SELECT * FROM {0} WHERE ticker = @ticker AND timestamp >= @from AND timestamp < @to",
            GetTickerForTest(), label, from, to));

        // Тест 3: Time-range query (1 час)
        var (fromHour, toHour) = GetHourRangeForTest();
        results.Add(await RunSingleReadTestAsync(
            tableName, "TimeRange_1hour", "SELECT * FROM {0} WHERE ticker = @ticker AND timestamp >= @from AND timestamp < @to",
            GetTickerForTest(), label, fromHour, toHour));

        // Тест 4: COUNT по тикеру с фильтром по времени
        results.Add(await RunSingleReadTestAsync(
            tableName, "Count_TimeRange", "SELECT COUNT(*) FROM {0} WHERE ticker = @ticker AND timestamp >= @from AND timestamp < @to",
            GetTickerForTest(), label, from, to));

        // Тест 5: SELECT с ORDER BY и LIMIT (top-N query)
        results.Add(await RunSingleReadTestAsync(
            tableName, "TopN_Limit1000", "SELECT * FROM {0} WHERE ticker = @ticker ORDER BY timestamp DESC LIMIT 1000",
            GetTickerForTest(), label));

        return results;
    }

    /// <summary>
    /// Запускает один READ-тест с усреднением по итерациям.
    /// </summary>
    private async Task<ReadBenchmarkResult> RunSingleReadTestAsync(
        string tableName, string testName, string sqlTemplate,
        string ticker, string label,
        DateTime? from = null, DateTime? to = null)
    {
        var sql = string.Format(sqlTemplate, tableName);
        var iterations = _config.ReadBenchmarkIterations;
        var times = new List<double>();
        long rowCount = 0;
        string? explainPlan = null;

        await using var conn = new NpgsqlConnection(_config.ConnectionString);
        await conn.OpenAsync();

        // EXPLAIN ANALYZE — один раз для плана запроса
        await using (var cmd = conn.CreateCommand())
        {
            var explainSql = $"EXPLAIN ANALYZE {sql}";
            cmd.CommandText = explainSql;
            cmd.Parameters.AddWithValue("@ticker", ticker);
            if (from.HasValue) cmd.Parameters.AddWithValue("@from", from.Value);
            if (to.HasValue) cmd.Parameters.AddWithValue("@to", to.Value);

            var plan = await cmd.ExecuteScalarAsync();
            explainPlan = plan?.ToString();
        }

        // Усреднение по итерациям
        for (int i = 0; i < iterations; i++)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("@ticker", ticker);
            if (from.HasValue) cmd.Parameters.AddWithValue("@from", from.Value);
            if (to.HasValue) cmd.Parameters.AddWithValue("@to", to.Value);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            if (sqlTemplate.Contains("COUNT(*)"))
            {
                var countResult = await cmd.ExecuteScalarAsync();
                rowCount = countResult is long l ? l : Convert.ToInt64(countResult);
            }
            else
            {
                await using var reader = await cmd.ExecuteReaderAsync();
                var rowCountLocal = 0L;
                while (await reader.ReadAsync()) { rowCountLocal++; }
                rowCount = rowCountLocal;
            }

            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds);
        }

        var avgMs = times.Average();
        var minMs = times.Min();
        var maxMs = times.Max();

        var result = new ReadBenchmarkResult(
            Table: tableName,
            Label: label,
            TestName: testName,
            Ticker: ticker,
            Iterations: iterations,
            AvgMs: avgMs,
            MinMs: minMs,
            MaxMs: maxMs,
            RowsReturned: rowCount,
            ExplainPlan: explainPlan);

        // Краткий вывод
        Console.WriteLine($"  {testName,-20} avg={avgMs,8:F2}ms  min={minMs,8:F2}ms  max={maxMs,8:F2}ms  rows={rowCount}");

        return result;
    }

    private string GetTickerForTest() => _config.Tickers.Length > 0 ? _config.Tickers[0] : "BENCHTEST";

    private (DateTime from, DateTime to) GetTimeRangeForTest()
    {
        var now = DateTime.UtcNow;
        var today = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc);
        return (today, today.AddDays(1));
    }

    private (DateTime from, DateTime to) GetHourRangeForTest()
    {
        var now = DateTime.UtcNow;
        var hourStart = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
        return (hourStart, hourStart.AddHours(1));
    }
}

public sealed record ReadBenchmarkResult(
    string Table,
    string Label,
    string TestName,
    string Ticker,
    int Iterations,
    double AvgMs,
    double MinMs,
    double MaxMs,
    long RowsReturned,
    string? ExplainPlan
);

public static class ReadResultsFormatter
{
    private const string Separator = "--------------------------------------------------------------------------------";

    public static void PrintComparison(List<ReadBenchmarkResult> baseline, List<ReadBenchmarkResult> partitioned)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 90));
        Console.WriteLine("  READ BENCHMARK COMPARISON: Regular Table vs Partitioned Table");
        Console.WriteLine(new string('=', 90));
        Console.WriteLine();
        Console.WriteLine($"{"Test",-22} {"Regular(ms)",-14} {"Partitioned(ms)",-16} {"Speedup",-10} {"Rows",-10}");
        Console.WriteLine(Separator);

        // Группируем по testName
        var baselineByTest = baseline.ToDictionary(r => r.TestName);
        var partitionedByTest = partitioned.ToDictionary(r => r.TestName);

        foreach (var testName in baselineByTest.Keys)
        {
            if (!partitionedByTest.TryGetValue(testName, out var part))
                continue;

            var reg = baselineByTest[testName];
            var speedup = reg.AvgMs > 0 ? reg.AvgMs / part.AvgMs : 0;

            Console.WriteLine(
                $"{testName,-22} {reg.AvgMs,10:F2}     {part.AvgMs,12:F2}     {speedup,6:F2}x    {reg.RowsReturned,8}");
        }

        Console.WriteLine(Separator);
        Console.WriteLine();
    }

    public static void PrintExplainPlans(List<ReadBenchmarkResult> results)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 90));
        Console.WriteLine("  EXPLAIN ANALYZE PLANS");
        Console.WriteLine(new string('=', 90));

        foreach (var r in results)
        {
            Console.WriteLine();
            Console.WriteLine($"  --- {r.Label} / {r.TestName} ---");
            if (r.ExplainPlan != null)
            {
                // Выводим только ключевые строки плана (Contains Index Scan, Seq Scan, Partition, etc.)
                var lines = r.ExplainPlan.Split('\n');
                foreach (var line in lines)
                {
                    if (line.Contains("Scan", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Partition", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Index", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Sort", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Aggregate", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Rows", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Planning Time", StringComparison.OrdinalIgnoreCase) ||
                        line.Contains("Execution Time", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"    {line.Trim()}");
                    }
                }
            }
            else
            {
                Console.WriteLine("    (no plan available)");
            }
        }

        Console.WriteLine();
    }
}
