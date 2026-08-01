using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Консольный вывод с цветовым оформлением и маппингом на уровни логирования.</summary>
public sealed class ConsoleUI : IConsoleUI
{
    private readonly ILogger<ConsoleUI> _logger;

    public ConsoleUI(ILogger<ConsoleUI> logger)
    {
        _logger = logger;
    }

    public void SectionHeader(string title)
    {
        ArgumentNullException.ThrowIfNull(title);

        string frame = new('=', Math.Max(title.Length, 20));
        _logger.LogInformation("{Frame}", frame);
        _logger.LogInformation("{Title}", title.ToUpperInvariant());
        _logger.LogInformation("{Frame}", frame);
    }

    public void Info(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _logger.LogInformation("{Message}", message);
    }

    public void Warn(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _logger.LogWarning("{Message}", message);
    }

    public void Ok(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _logger.LogInformation("[OK] {Message}", message);
    }

    public void Error(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _logger.LogError("{Message}", message);
    }

    public void Detail(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        _logger.LogDebug("{Message}", message);
    }
}
