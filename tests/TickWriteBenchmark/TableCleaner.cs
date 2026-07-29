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
    /// Очищает таблицу через TRUNCATE.
    /// </summary>
    /// <param name="tableName">Имя таблицы (по умолчанию rawticks).</param>
    public async Task TruncateAsync(string tableName = "rawticks")
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"TRUNCATE TABLE {tableName};";
        await cmd.ExecuteNonQueryAsync();
    }
}
