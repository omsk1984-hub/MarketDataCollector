namespace TickWriteBenchmark;

public sealed class BenchmarkConfig
{
    public string ConnectionString { get; init; } =
        "Host=localhost;Port=5433;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!;sslmode=Disable;No Reset On Close=true;Keepalive=30";

    /// <summary>
    /// Размеры чанков для тестирования.
    /// </summary>
    public int[] ChunkSizes { get; init; } = [ 500, 700, 800, 900, 1000];

    /// <summary>
    /// Фиксированное общее количество тиков на каждый тест.
    /// </summary>
    public int TotalTicks { get; init; } = 20000;
}
