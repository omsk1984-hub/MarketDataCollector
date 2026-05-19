using Npgsql;

namespace TickWriteBenchmark;

public sealed class TableCleaner
{
    private readonly string _connectionString;

    public TableCleaner(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Очищает таблицу rawticks через TRUNCATE.
    /// </summary>
    public async Task TruncateAsync()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE rawticks;";
        await cmd.ExecuteNonQueryAsync();
    }
}
