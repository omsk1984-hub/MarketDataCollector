using System.Text;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// Генерация topN-отчёта по trace через <c>dotnet-trace report topN</c>.
/// Запускает команду по собранному .nettrace и сохраняет результат в Markdown.
/// </summary>
public sealed class TopNReporter : ITopNReporter
{
    private readonly IToolRunner _toolRunner;
    private readonly IConsoleUI _ui;
    private readonly ILogger<TopNReporter> _logger;
    private readonly ProfilerOptions _options;

    public TopNReporter(
        IToolRunner toolRunner,
        IConsoleUI ui,
        ILogger<TopNReporter> logger,
        ProfilerOptions options)
    {
        _toolRunner = toolRunner;
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public async Task<string> GenerateAsync(string traceFile, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(traceFile);

        if (!File.Exists(traceFile))
        {
            _ui.Warn($"Trace-файл не найден для topN-анализа: {traceFile}");
            return string.Empty;
        }

        string topNArg = $"-n {_options.TopNCount}";
        if (_options.TopNInclusive)
        {
            topNArg += " --inclusive";
        }

        string args = $"report \"{traceFile}\" topN {topNArg}";

        _ui.Info($"Запуск dotnet-trace report topN (top {_options.TopNCount}) ...");
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", args, cancellationToken);

        string stdout = await run.StdOutTask;
        string stderr = await run.StdErrTask;
        await run.Process.WaitForExitAsync(cancellationToken);

        _logger.LogDebug("dotnet-trace report topN ExitCode={ExitCode}.", run.Process.ExitCode);

        if (run.Process.ExitCode != 0)
        {
            _ui.Warn("dotnet-trace report topN завершился с ошибкой. Trace может не содержать CPU-событий.");
            if (!string.IsNullOrWhiteSpace(stderr))
            {
                _ui.Detail(stderr);
            }

            return string.Empty;
        }

        if (string.IsNullOrWhiteSpace(stdout))
        {
            _ui.Warn("dotnet-trace report topN вернул пустой вывод — trace не содержит CPU-стеков.");
            return string.Empty;
        }

        string reportPath = Path.Combine(
            Path.GetDirectoryName(traceFile) ?? ".",
            $"{Path.GetFileNameWithoutExtension(traceFile)}_topn.md");

        await File.WriteAllTextAsync(
            reportPath,
            BuildMarkdown(reportPath, stdout),
            System.Text.Encoding.UTF8,
            cancellationToken);

        _ui.Ok($"topN-отчёт создан: {reportPath}");
        _logger.LogDebug("topN-отчёт записан в {Path}", reportPath);

        return reportPath;
    }

    private string BuildMarkdown(string reportPath, string topNOutput)
    {
        using var writer = new StringWriter();

        writer.WriteLine("# topN — топ методов на call stack (dotnet-trace report)");
        writer.WriteLine();
        writer.WriteLine($"Trace: `{reportPath}`");
        writer.WriteLine();
        writer.WriteLine($"Профиль: `{_options.TraceProfile}`");
        writer.WriteLine($"Top-N: `{_options.TopNCount}` ({( _options.TopNInclusive ? "inclusive" : "exclusive")})");
        writer.WriteLine();
        writer.WriteLine("## Вывод dotnet-trace report topN");
        writer.WriteLine();
        writer.WriteLine("```");
        writer.WriteLine(topNOutput.TrimEnd());
        writer.WriteLine("```");
        writer.WriteLine();
        writer.WriteLine("## Примечания");
        writer.WriteLine();
        writer.WriteLine("- Отчёт даёт топ методов по времени на call stack (CPU-сamples).");
        writer.WriteLine("- Для анализа аллокаций по типам/стекам и contention-стеков используйте PerfView");
        writer.WriteLine("  (см. план `plans/perfview-analysis-plan.md`).");
        writer.WriteLine("- `dotnet-trace report` в установленной версии поддерживает только `topN`.");
        writer.WriteLine();

        return writer.ToString();
    }
}