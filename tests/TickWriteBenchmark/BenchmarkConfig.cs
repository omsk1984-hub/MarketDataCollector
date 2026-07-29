namespace TickWriteBenchmark;

public sealed class BenchmarkConfig
{
    public string ConnectionString { get; init; } =
        "Host=localhost;Port=5433;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!;sslmode=Disable;No Reset On Close=true;Keepalive=30";

    /// <summary>
    /// Размеры чанков для тестирования.
    /// </summary>
    public int[] ChunkSizes { get; init; } = [ 500, 1000, 2000, 5000];

    /// <summary>
    /// Фиксированное общее количество тиков на каждый тест.
    /// </summary>
    public int TotalTicks { get; init; } = 500000;

    /// <summary>
    /// Тикеры для генерации данных (мульти-тикери как в реальности).
    /// </summary>
    public string[] Tickers { get; init; } = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

    /// <summary>
    /// Биржа для генерации данных.
    /// </summary>
    public string Exchange { get; init; } = "BENCH";

    /// <summary>
    /// Количество итераций для READ-бенчмарка (усреднение результатов).
    /// </summary>
    public int ReadBenchmarkIterations { get; init; } = 5;
}
