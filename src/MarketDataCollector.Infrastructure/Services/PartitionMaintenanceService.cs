using MarketDataCollector.Core.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MarketDataCollector.Infrastructure.Services;

/// <summary>
/// Hosted-сервис автоматического обслуживания партиций таблицы rawticks.
///
/// Задачи:
///  1. Создание партиций вперёд (premake) — чтобы INSERT-ы всегда попадали
///     в существующую партицию и не уходили в default/бесконечный рост.
///  2. Удаление партиций старше retention — политика хранения данных.
///
/// Не использует внешние расширения Postgres (pg_partman/pg_cron) — вся
/// логика на чистом SQL, выполняется по расписанию внутри воркера.
/// Безопасность: перед удалением партиций проверяется, что parent-таблица
/// действительно партиционирована (наличие в pg_partitioned_table).
/// </summary>
public sealed class PartitionMaintenanceService : BackgroundService
{
    private readonly ILogger<PartitionMaintenanceService> _logger;
    private readonly PartitioningOptions _options;
    private readonly string _connectionString;

    public PartitionMaintenanceService(
        ILogger<PartitionMaintenanceService> logger,
        IOptions<PartitioningOptions> options,
        string connectionString)
    {
        _logger = logger;
        _options = options.Value;
        _connectionString = connectionString;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("PartitionMaintenanceService disabled by configuration.");
            return;
        }

        _logger.LogInformation(
            "PartitionMaintenanceService started. Table={Table} partition_col={Col} premake={Premake}d retention={Retention}d interval={Interval}",
            _options.TableName, _options.PartitionColumn, _options.PremakeDays,
            _options.RetentionDays, _options.MaintenanceInterval);

        // Выполняем сразу при старте, затем по расписанию.
        try
        {
            await RunMaintenanceAsync(stoppingToken);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "PartitionMaintenanceService initial run failed");
        }

        using var timer = new PeriodicTimer(_options.MaintenanceInterval);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await timer.WaitForNextTickAsync(stoppingToken);
                await RunMaintenanceAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PartitionMaintenanceService maintenance run failed");
            }
        }

        _logger.LogInformation("PartitionMaintenanceService stopped.");
    }

    private async Task RunMaintenanceAsync(CancellationToken ct)
    {
        var table = _options.TableName;
        var column = _options.PartitionColumn;

        // Убеждаемся, что таблица существует и партиционирована.
        if (!await IsPartitionedAsync(table, ct))
        {
            _logger.LogWarning(
                "Table '{Table}' not found or not partitioned. Skipping partition maintenance. " +
                "Ensure EF Core migrations created it as PARTITION BY RANGE ({Col}).",
                table, column);
            return;
        }

        var created = 0;
        var dropped = 0;
        var today = DateTime.UtcNow.Date;

        // 1) Создание партиций вперёд: [today, today + premake)
        for (var day = 0; day < _options.PremakeDays; day++)
        {
            var from = today.AddDays(day);
            var to = from.AddDays(1);
            var partitionName = $"{table}_{from:yyyy_MM_dd}";
            var sql =
                $"CREATE TABLE IF NOT EXISTS \"{partitionName}\" " +
                $"PARTITION OF \"{table}\" " +
                $"FOR VALUES FROM ('{from:yyyy-MM-dd}') TO ('{to:yyyy-MM-dd}');";
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(ct);
            created++;
        }

        // 2) Удаление партиций старше retention (только при наличии разрешения).
        if (_options.AllowDropPartitions && _options.RetentionDays > 0)
        {
            var retentionCutoff = today.AddDays(-_options.RetentionDays);
            // Получаем список дочерних таблиц-партиций с границами.
            var partitions = await GetPartitionsAsync(table, ct);
            foreach (var p in partitions)
            {
                // Имя формата <table>_yyyy_MM_dd. Извлекаем дату из имени.
                var suffix = p.Substring(table.Length + 1);
                if (DateTime.TryParseExact(suffix, "yyyy_MM_dd",
                        System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.None, out var partDate))
                {
                    if (partDate < retentionCutoff)
                    {
                        await using var conn = new NpgsqlConnection(_connectionString);
                        await conn.OpenAsync(ct);
                        var sql = $"DROP TABLE IF EXISTS \"{p}\";";
                        await using var cmd = new NpgsqlCommand(sql, conn);
                        await cmd.ExecuteNonQueryAsync(ct);
                        _logger.LogInformation("Partition dropped: {Partition}", p);
                        dropped++;
                    }
                }
            }
        }

        _logger.LogInformation("Partition maintenance done: created={Created} dropped={Dropped}", created, dropped);
    }

    private async Task<bool> IsPartitionedAsync(string table, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        const string sql = @"
            SELECT EXISTS (
                SELECT 1 FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = @table
            );";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@table", table);
        return (bool)(await cmd.ExecuteScalarAsync(ct) ?? false);
    }

    private async Task<IReadOnlyList<string>> GetPartitionsAsync(string table, CancellationToken ct)
    {
        var result = new List<string>();
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        const string sql = @"
            SELECT child.relname
            FROM pg_inherits
            JOIN pg_class child ON child.oid = pg_inherits.inhrelid
            JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
            JOIN pg_namespace n ON n.oid = parent.relnamespace
            WHERE parent.relname = @table;";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@table", table);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(reader.GetString(0));
        }
        return result;
    }
}
