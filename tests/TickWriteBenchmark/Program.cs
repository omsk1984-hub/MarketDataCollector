namespace TickWriteBenchmark;

public static class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("========================================");
        Console.WriteLine("  Tick Write Benchmark");
        Console.WriteLine("========================================");
        Console.WriteLine();

        var config = new BenchmarkConfig();
        var generator = new TickDataGenerator();
        var cleaner = new TableCleaner(config.ConnectionString);

        Console.WriteLine($"Connection: {config.ConnectionString}");
        Console.WriteLine($"Total ticks per test: {config.TotalTicks}");
        Console.WriteLine($"Chunk sizes: {string.Join(", ", config.ChunkSizes)}");
        Console.WriteLine();

        var runner = new BenchmarkRunner(config, generator, cleaner);

        try
        {
            var results = await runner.RunAllBenchmarksAsync();
            ResultsFormatter.Print(results);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR: {ex}");
        }

        if (!Console.IsInputRedirected)
        {
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
