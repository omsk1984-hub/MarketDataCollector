using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Конвертация .nettrace в SpeedScope-формат через dotnet-trace.</summary>
public sealed class SpeedScopeConverter : ISpeedScopeConverter
{
    private readonly IToolRunner _toolRunner;
    private readonly IConsoleUI _ui;
    private readonly ILogger<SpeedScopeConverter> _logger;

    public SpeedScopeConverter(IToolRunner toolRunner, IConsoleUI ui, ILogger<SpeedScopeConverter> logger)
    {
        _toolRunner = toolRunner;
        _ui = ui;
        _logger = logger;
    }

    public async Task<string> ConvertAsync(string traceFile, CancellationToken cancellationToken)
    {
        if (!File.Exists(traceFile))
        {
            _ui.Warn($"Trace-файл не найден для конвертации: {traceFile}");
            return string.Empty;
        }

        // dotnet-trace convert требует путь без расширения как базу вывода.
        string basePath = Path.Combine(
            Path.GetDirectoryName(traceFile) ?? ".",
            Path.GetFileNameWithoutExtension(traceFile));

        string args = $"convert --format speedscope \"{traceFile}\" --output \"{basePath}\"";

        _ui.Info("Конвертация trace в SpeedScope ...");
        ToolRun run = await _toolRunner.RunAsync("dotnet-trace", args, cancellationToken);
        await run.Process.WaitForExitAsync(cancellationToken);

        string stdout = await run.StdOutTask;
        string stderr = await run.StdErrTask;

        _logger.LogDebug("convert ExitCode={ExitCode}.", run.Process.ExitCode);

        // Из-за возможного дублирования имени dotnet-trace добавляет суффикс. Ищем фактический файл.
        string resultPath = FindSpeedScopeFile(basePath);

        if (string.IsNullOrEmpty(resultPath))
        {
            _ui.Warn("Файл SpeedScope не создан. Возможен повреждённый trace (broken/best-effort).");
            _ui.Detail(stdout);
            if (!string.IsNullOrWhiteSpace(stderr))
            {
                _ui.Detail(stderr);
            }

            return string.Empty;
        }

        long size = new FileInfo(resultPath).Length;
        _ui.Ok($"SpeedScope создан: {resultPath} ({size:N0} байт).");
        return resultPath;
    }

    /// <summary>Ищет фактический результат конвертации рядом с базовым путём.</summary>
    private static string FindSpeedScopeFile(string basePath)
    {
        string? dir = Path.GetDirectoryName(basePath);
        if (string.IsNullOrEmpty(dir))
        {
            return string.Empty;
        }

        string fileName = Path.GetFileName(basePath);

        // Возможные варианты вывода.
        string[] candidates =
        {
            $"{fileName}.speedscope.json",
            $"{fileName}.speedscope",
            $"{fileName}.json",
        };

        foreach (string candidate in candidates)
        {
            string full = Path.Combine(dir, candidate);
            if (File.Exists(full))
            {
                return full;
            }
        }

        // Иначе ищем любой файл, начинающийся с имени и содержащий "speedscope".
        return Directory.EnumerateFiles(dir)
            .FirstOrDefault(f => Path.GetFileName(f).StartsWith(fileName, StringComparison.OrdinalIgnoreCase))
            ?? string.Empty;
    }
}
