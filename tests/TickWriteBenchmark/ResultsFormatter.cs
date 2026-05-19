namespace TickWriteBenchmark;

public sealed record BenchmarkResult(
    string Method,
    string Mode,
    int ChunkSize,
    int TotalTicks,
    int ChunksCount,
    double ElapsedMs,
    double TicksPerSec
);

public static class ResultsFormatter
{
    private const string Separator = "--------------------------------------------------------------------------------";

    public static void Print(List<BenchmarkResult> results)
    {
        Console.WriteLine();
        Console.WriteLine(new string('=', 80));
        Console.WriteLine("  TICK WRITE BENCHMARK RESULTS  (8000 ticks per test)");
        Console.WriteLine(new string('=', 80));
        Console.WriteLine();
        Console.WriteLine($"{"Method",-25} {"Mode",-12} {"ChunkSize",-10} {"Chunks",-8} {"Time(ms)",-10} {"Ticks/sec",-12}");
        Console.WriteLine(Separator);

        // Группировка по методу, внутри сначала sequential, потом parallel, по возрастанию чанка
        var ordered = results
            .GroupBy(r => r.Method)
            .SelectMany(g =>
                g.OrderBy(r => r.Mode == "Sequential" ? 0 : 1)
                 .ThenBy(r => r.ChunkSize));

        foreach (var r in ordered)
        {
            var tickPerSecStr = r.TicksPerSec switch
            {
                >= 10000 => $"{r.TicksPerSec,8:F0}",
                >= 1000 => $"{r.TicksPerSec,8:F1}",
                _ => $"{r.TicksPerSec,8:F1}"
            };

            Console.WriteLine(
                $"{r.Method,-25} {r.Mode,-12} {r.ChunkSize,-10} {r.ChunksCount,-8} {r.ElapsedMs,8:F1}  {tickPerSecStr}");
        }

        Console.WriteLine(Separator);
        Console.WriteLine();
        PrintBestResults(results);
    }

    private static void PrintBestResults(List<BenchmarkResult> results)
    {
        // Лучший sequential
        var bestSeq = results
            .Where(r => r.Mode == "Sequential")
            .MaxBy(r => r.TicksPerSec);

        // Лучший parallel
        var bestPar = results
            .Where(r => r.Mode == "Parallel")
            .MaxBy(r => r.TicksPerSec);

        Console.WriteLine("  BEST RESULTS:");
        if (bestSeq is not null)
            Console.WriteLine($"    Sequential: {bestSeq.Method,-23} chunk={bestSeq.ChunkSize,-4}  {bestSeq.TicksPerSec,8:F0} ticks/sec");
        if (bestPar is not null)
            Console.WriteLine($"    Parallel:   {bestPar.Method,-23} chunk={bestPar.ChunkSize,-4}  {bestPar.TicksPerSec,8:F0} ticks/sec");
        Console.WriteLine();
    }
}
