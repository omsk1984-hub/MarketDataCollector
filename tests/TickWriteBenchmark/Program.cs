namespace TickWriteBenchmark;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("  Tick Write & Read Benchmark");
        Console.WriteLine("========================================");
        Console.WriteLine();

        var config = new BenchmarkConfig();
        var generator = new TickDataGenerator();
        var cleaner = new TableCleaner(config.ConnectionString);

        Console.WriteLine($"Connection: {config.ConnectionString}");
        Console.WriteLine($"Total ticks per write test: {config.TotalTicks:N0}");
        Console.WriteLine($"Chunk sizes: {string.Join(", ", config.ChunkSizes)}");
        Console.WriteLine($"Tickers: {string.Join(", ", config.Tickers)}");
        Console.WriteLine($"Read benchmark iterations: {config.ReadBenchmarkIterations}");
        Console.WriteLine();

        // =====================
        // Фаза 1: WRITE-бенчмарк
        // =====================
        Console.WriteLine("========================================");
        Console.WriteLine("  PHASE 1: WRITE BENCHMARK");
        Console.WriteLine("========================================");

        var writeRunner = new BenchmarkRunner(config, generator, cleaner);

        try
        {
            var writeResults = await writeRunner.RunAllBenchmarksAsync();
            ResultsFormatter.Print(writeResults);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"WRITE BENCHMARK ERROR: {ex}");
        }

        // =====================
        // Фаза 2: READ-бенчмарк (baseline — обычная таблица rawticks)
        // =====================
        Console.WriteLine();
        Console.WriteLine("========================================");
        Console.WriteLine("  PHASE 2: READ BENCHMARK (Regular Table)");
        Console.WriteLine("========================================");

        var readRunner = new ReadBenchmarkRunner(config, generator, cleaner);

        try
        {
            // Заполняем таблицу данными для READ-тестов
            await cleaner.TruncateAsync();
            await readRunner.SeedDataAsync("rawticks");

            var baselineResults = await readRunner.RunReadBenchmarksAsync("rawticks", "Regular");

            // Сохраняем baseline для сравнения
            var baselineFile = "read_baseline.json";
            await SaveResultsAsync(baselineFile, baselineResults);
            Console.WriteLine($"  Baseline results saved to {baselineFile}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"READ BENCHMARK (baseline) ERROR: {ex}");
        }

        // =====================
        // Фаза 3: READ-бенчмарк (партиционированная таблица)
        // =====================
        Console.WriteLine();
        Console.WriteLine("========================================");
        Console.WriteLine("  PHASE 3: READ BENCHMARK (Partitioned Table)");
        Console.WriteLine("========================================");

        try
        {
            // Проверяем, существует ли партиционированная таблица
            var partitionedExists = await TableExistsAsync(config.ConnectionString, "rawticks_partitioned");

            if (partitionedExists)
            {
                await cleaner.TruncateAsync("rawticks_partitioned");
                await readRunner.SeedDataAsync("rawticks_partitioned");

                var partitionedResults = await readRunner.RunReadBenchmarksAsync("rawticks_partitioned", "Partitioned");

                // Сохраняем partitioned результаты
                var partitionedFile = "read_partitioned.json";
                await SaveResultsAsync(partitionedFile, partitionedResults);
                Console.WriteLine($"  Partitioned results saved to {partitionedFile}");

                // Сравнение
                var baselineResults = await LoadResultsAsync("read_baseline.json");
                if (baselineResults != null)
                {
                    ReadResultsFormatter.PrintComparison(baselineResults, partitionedResults);

                    // EXPLAIN ANALYZE планы
                    ReadResultsFormatter.PrintExplainPlans(baselineResults);
                    ReadResultsFormatter.PrintExplainPlans(partitionedResults);
                }
            }
            else
            {
                Console.WriteLine("  Partitioned table 'rawticks_partitioned' not found.");
                Console.WriteLine("  Run docker/init-partitioned.sql first to create it.");
                Console.WriteLine("  Skipping partitioned read benchmark.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"READ BENCHMARK (partitioned) ERROR: {ex}");
        }

        Console.WriteLine();
        Console.WriteLine("========================================");
        Console.WriteLine("  BENCHMARK COMPLETE");
        Console.WriteLine("========================================");

        if (!Console.IsInputRedirected)
        {
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }

    private static async Task<bool> TableExistsAsync(string connStr, string tableName)
    {
        await using var conn = new Npgsql.NpgsqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = @tableName
            );";
        cmd.Parameters.AddWithValue("@tableName", tableName);
        var result = await cmd.ExecuteScalarAsync();
        return result is bool b && b;
    }

    private static async Task SaveResultsAsync(string fileName, object results)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(results, new System.Text.Json.JsonSerializerOptions
        {
            WriteIndented = true
        });
        await File.WriteAllTextAsync(fileName, json);
    }

    private static async Task<List<ReadBenchmarkResult>?> LoadResultsAsync(string fileName)
    {
        if (!File.Exists(fileName))
            return null;

        var json = await File.ReadAllTextAsync(fileName);
        return System.Text.Json.JsonSerializer.Deserialize<List<ReadBenchmarkResult>>(json);
    }
}
