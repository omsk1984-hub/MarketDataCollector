using System.Text.RegularExpressions;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Парсер Prometheus-текстового формата exposition.</summary>
public sealed partial class PrometheusParser : IPrometheusParser
{
    private readonly ILogger<PrometheusParser> _logger;

    public PrometheusParser(ILogger<PrometheusParser> logger)
    {
        _logger = logger;
    }

    public IReadOnlyList<MetricSample> Parse(string body)
    {
        ArgumentNullException.ThrowIfNull(body);

        var samples = new List<MetricSample>();
        Dictionary<string, string> descriptions = new(StringComparer.Ordinal);
        Dictionary<string, string> types = new(StringComparer.Ordinal);
        string timestamp = DateTime.UtcNow.ToString("o");

        // Первый проход: директивы # HELP и # TYPE.
        using (StringReader reader = new(body))
        {
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                string trimmed = line.Trim();
                if (!trimmed.StartsWith('#'))
                {
                    continue;
                }

                string[] parts = trimmed.Split(' ', 4, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 3)
                {
                    continue;
                }

                string directive = parts[1];
                string metricName = parts[2];

                if (directive.Equals("HELP", StringComparison.OrdinalIgnoreCase))
                {
                    string description = parts.Length > 3 ? string.Join(' ', parts[3..]) : string.Empty;
                    descriptions[metricName] = description;
                }
                else if (directive.Equals("TYPE", StringComparison.OrdinalIgnoreCase))
                {
                    string type = parts.Length > 3 ? parts[3] : string.Empty;
                    types[metricName] = type;
                }
            }
        }

        // Второй проход: сами образцы.
        using (StringReader reader = new(body))
        {
            string? line;
            while ((line = reader.ReadLine()) is not null)
            {
                string trimmed = line.Trim();
                if (trimmed.Length == 0 || trimmed.StartsWith('#'))
                {
                    continue;
                }

                Match match = SampleRegex().Match(trimmed);
                if (!match.Success)
                {
                    continue;
                }

                string metric = match.Groups["name"].Value;
                string labels = match.Groups["labels"].Success ? match.Groups["labels"].Value : string.Empty;
                string value = match.Groups["value"].Value;

                descriptions.TryGetValue(metric, out string? description);
                types.TryGetValue(metric, out string? type);

                samples.Add(new MetricSample(
                    Timestamp: timestamp,
                    Metric: metric,
                    Labels: labels,
                    Type: type ?? string.Empty,
                    Value: NormalizeValue(value),
                    Description: description ?? string.Empty));
            }
        }

        _logger.LogDebug("Разобрано образцов метрик: {Count}", samples.Count);
        return samples;
    }

    /// <summary>Нормализует специальные значения NaN/Inf и экспоненциальную запись.</summary>
    private static string NormalizeValue(string raw)
    {
        string value = raw.Trim();
        return value switch
        {
            "NaN" or "nan" => "NaN",
            "+Inf" or "Inf" or "inf" => "Inf",
            "-Inf" or "-inf" => "-Inf",
            _ => value,
        };
    }

    // Имя метрики, опциональные {labels}, значение (включая NaN/Inf и экспоненциальную запись).
    [GeneratedRegex(
        @"^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(\{(?<labels>[^}]*)\})?\s+(?<value>[+\-]?[0-9.eE+\-]+|NaN|Inf|-Inf)\s*$",
        RegexOptions.Compiled)]
    private static partial Regex SampleRegex();
}
