namespace MarketDataCollector.Core.Configuration;

public class ExchangeConfig
{
    public string ExchangeName { get; set; } = string.Empty;
    public string WebSocketUrl { get; set; } = string.Empty;
    public string? Symbol { get; set; }
}

public class ReaderConfig
{
    public string ExchangeName { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
}

/// <summary>
/// Корневой класс конфигурации для привязки секции "Exchanges" и "Readers" через IOptions<T>
/// </summary>
public class ExchangeOptions
{
    public const string SectionName = "ExchangeOptions";

    public List<ExchangeConfig> Exchanges { get; set; } = new();
    public List<ReaderConfig> Readers { get; set; } = new();
}
