using System.Diagnostics;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Проверяет и при необходимости доустанавливает глобальные dotnet-инструменты.</summary>
public sealed class EnsureDotnetTools : IEnsureDotnetTools
{
    private static readonly string[] RequiredTools = { "dotnet-trace", "dotnet-gcdump" };

    private readonly IToolRunner _toolRunner;
    private readonly IConsoleUI _ui;
    private readonly ILogger<EnsureDotnetTools> _logger;

    public EnsureDotnetTools(IToolRunner toolRunner, IConsoleUI ui, ILogger<EnsureDotnetTools> logger)
    {
        _toolRunner = toolRunner;
        _ui = ui;
        _logger = logger;
    }

    public async Task EnsureAsync(CancellationToken cancellationToken)
    {
        string installed = await RunListAsync(cancellationToken);

        foreach (string tool in RequiredTools)
        {
            if (installed.Contains(tool, StringComparison.OrdinalIgnoreCase))
            {
                _ui.Detail($"Инструмент {tool} уже установлен.");
                continue;
            }

            _ui.Info($"Устанавливаю глобальный инструмент: {tool} ...");
            await InstallAsync(tool, cancellationToken);
        }

        _ui.Ok("Все dotnet-инструменты доступны.");
    }

    private async Task<string> RunListAsync(CancellationToken cancellationToken)
    {
        ToolRun run = await _toolRunner.RunAsync("dotnet", "tool list --global", cancellationToken);
        await run.Process.WaitForExitAsync(cancellationToken);

        string stdout = await run.StdOutTask;
        if (!string.IsNullOrWhiteSpace(await run.StdErrTask))
        {
            _logger.LogDebug("stderr от 'dotnet tool list': {Err}", await run.StdErrTask);
        }

        return stdout;
    }

    private async Task InstallAsync(string tool, CancellationToken cancellationToken)
    {
        ToolRun run = await _toolRunner.RunAsync("dotnet", $"tool install --global {tool}", cancellationToken);
        await run.Process.WaitForExitAsync(cancellationToken);

        string stdout = await run.StdOutTask;
        string stderr = await run.StdErrTask;

        if (run.Process.ExitCode != 0)
        {
            _ui.Error($"Не удалось установить {tool}. Код выхода: {run.Process.ExitCode}.");
            _ui.Detail(stdout);
            if (!string.IsNullOrWhiteSpace(stderr))
            {
                _ui.Detail(stderr);
            }

            Environment.Exit(1);
        }
    }
}
