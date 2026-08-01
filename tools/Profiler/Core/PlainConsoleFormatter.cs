using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;
using Microsoft.Extensions.Options;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Консольный форматтер без категории и уровня логирования.
/// Выводит только timestamp и сообщение, что убирает префикс
/// вида <c>MarketDataCollector.Profiler.Core.ConsoleUI[0]</c>.
/// </summary>
public sealed class PlainConsoleFormatter : ConsoleFormatter
{
    private readonly PlainConsoleFormatterOptions _options;

    public PlainConsoleFormatter(IOptions<PlainConsoleFormatterOptions> options)
        : base(nameof(PlainConsoleFormatter))
    {
        _options = options.Value;
    }

    public override void Write<TState>(
        in LogEntry<TState> logEntry,
        IExternalScopeProvider? scopeProvider,
        TextWriter textWriter)
    {
        ArgumentNullException.ThrowIfNull(logEntry.Formatter);

        string message = logEntry.Formatter(logEntry.State, logEntry.Exception);
        if (string.IsNullOrEmpty(message))
        {
            return;
        }

        if (_options.TimestampFormat is not null)
        {
            textWriter.Write(DateTimeOffset.Now.ToString(_options.TimestampFormat));
        }

        textWriter.WriteLine(message);
    }
}

/// <summary>Опции форматтера <see cref="PlainConsoleFormatter"/>.</summary>
public sealed class PlainConsoleFormatterOptions : ConsoleFormatterOptions
{
    public PlainConsoleFormatterOptions()
    {
        TimestampFormat = "HH:mm:ss ";
    }
}
