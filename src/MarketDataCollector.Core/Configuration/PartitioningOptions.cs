namespace MarketDataCollector.Core.Configuration;

/// <summary>
/// Настройки автоматического партиционирования таблицы rawticks
/// (PartitionMaintenanceService).
/// </summary>
public class PartitioningOptions
{
    public const string SectionName = "Partitioning";

    /// <summary>Включить автоматическое обслуживание партиций.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Интервал запуска обслуживания партиций.</summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>На сколько дней вперёд создавать партиции (premake).</summary>
    public int PremakeDays { get; set; } = 7;

    /// <summary>Сколько дней хранить данные (retention). Партиции старше удаляются.</summary>
    public int RetentionDays { get; set; } = 30;

    /// <summary>Имя таблицы, партиции которой обслуживаются.</summary>
    public string TableName { get; set; } = "rawticks";

    /// <summary>Имя колонки-партиционирования.</summary>
    public string PartitionColumn { get; set; } = "timestamp";

    /// <summary>Защита: не удалять партиции, если таблица не партиционирована (безопасность).</summary>
    public bool AllowDropPartitions { get; set; } = true;
}
