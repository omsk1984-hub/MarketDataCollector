using System.Text;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Reporting;

/// <summary>Генерация Markdown-отчёта по результатам профилирования.</summary>
public sealed class ReportGenerator : IReportGenerator
{
    private readonly IConsoleUI _ui;
    private readonly ILogger<ReportGenerator> _logger;
    private readonly ProfilerOptions _options;

    public ReportGenerator(
        IConsoleUI ui,
        ILogger<ReportGenerator> logger,
        ProfilerOptions options)
    {
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public async Task<string> GenerateAsync(
        string outputDir,
        string runTimestamp,
        IReadOnlyList<(string Name, string Path)> outputFiles,
        IReadOnlyList<string> warnings,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outputFiles);
        ArgumentNullException.ThrowIfNull(warnings);

        string timestamp = runTimestamp;
        string reportPath = Path.Combine(outputDir, $"profiling_report_{timestamp}.md");

        var sb = new StringBuilder();

        sb.AppendLine("# Отчёт профилирования");
        sb.AppendLine();
        sb.AppendLine($"Дата/время: {DateTime.Now:dd.MM.yyyy HH:mm:ss}");
        sb.AppendLine();

        // Конфигурация.
        sb.AppendLine("## Конфигурация");
        sb.AppendLine();
        sb.AppendLine("| Параметр | Значение |");
        sb.AppendLine("|----------|----------|");
        sb.AppendLine($"| Режим | all |");
        sb.AppendLine($"| Trace Profile | {_options.TraceProfile} |");
        sb.AppendLine($"| Trace Duration (сек) | {_options.TraceDuration} |");
        sb.AppendLine($"| GcDump at Peak (сек) | {_options.GcDumpAtPeakSec} |");
        sb.AppendLine($"| Drain Wait (сек) | {_options.DrainWaitSec} |");
        sb.AppendLine();

        // Выходные файлы.
        sb.AppendLine("## Выходные файлы");
        sb.AppendLine();
        sb.AppendLine("| Файл | Размер |");
        sb.AppendLine("|------|--------|");

        foreach ((string name, string path) in outputFiles)
        {
            string size = FormatSize(GetFileSize(path));
            sb.AppendLine($"| {name} | {size} |");
        }

        sb.AppendLine();

        // Предупреждения.
        sb.AppendLine("## Предупреждения сбора");
        sb.AppendLine();
        if (warnings.Count == 0)
        {
            sb.AppendLine("_Нет предупреждений._");
        }
        else
        {
            foreach (string warning in warnings)
            {
                sb.AppendLine($"- {warning}");
            }
        }

        sb.AppendLine();

        // Анализ.
        sb.AppendLine("## Анализ");
        sb.AppendLine();
        sb.AppendLine("### dotnet-trace");
        sb.AppendLine();
        sb.AppendLine("Для анализа trace используйте PerfView или: ");
        sb.AppendLine("```bash");
        sb.AppendLine($"dotnet-trace report \"{Path.Combine(outputDir, "allocation_trace_*.nettrace")}\"");
        sb.AppendLine("```");
        sb.AppendLine();

        sb.AppendLine("### dotnet-gcdump");
        sb.AppendLine();
        sb.AppendLine("Откройте gcdump-файлы в Visual Studio (Инструменты -> Анализ дампа) или dotnet-dump:");
        sb.AppendLine("```bash");
        sb.AppendLine($"dotnet-dump analyze \"{Path.Combine(outputDir, "snapshot_peak_*.gcdump")}\"");
        sb.AppendLine($"dotnet-dump analyze \"{Path.Combine(outputDir, "snapshot_drained_*.gcdump")}\"");
        sb.AppendLine("```");
        sb.AppendLine();

        // Следующие шаги.
        sb.AppendLine("## Следующие шаги");
        sb.AppendLine();
        sb.AppendLine("1. Изучите SpeedScope-файл для визуализации флеймграфа.");
        sb.AppendLine("2. Сравните snapshot_peak и snapshot_drained для оценки накопления объектов.");
        sb.AppendLine("3. Проанализируйте counters CSV для корреляции нагрузки и задержек.");
        sb.AppendLine();

        Directory.CreateDirectory(outputDir);
        await File.WriteAllTextAsync(reportPath, sb.ToString(), Encoding.UTF8, cancellationToken);

        _ui.Ok($"Отчёт создан: {reportPath}");
        _logger.LogDebug("Отчёт записан в {Path}", reportPath);

        return reportPath;
    }

    private static long GetFileSize(string? path)
    {
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
        {
            return 0;
        }

        return new FileInfo(path).Length;
    }

    private static string FormatSize(long bytes)
    {
        if (bytes >= 1024 * 1024)
        {
            return $"{bytes / 1024.0 / 1024.0:F2} MB";
        }

        if (bytes >= 1024)
        {
            return $"{bytes / 1024.0:F2} KB";
        }

        return $"{bytes} B";
    }
}
