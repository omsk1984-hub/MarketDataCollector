using System.Text;
using MarketDataCollector.Profiler.Cli;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.DependencyInjection;

namespace MarketDataCollector.Profiler;

/// <summary>Точка входа утилиты профилирования.</summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        // Разбор аргументов; при ошибке/--help выходит внутри.
        ProfilerOptions options = CommandLineParser.Parse(args);

        using ServiceProvider provider = DiContainer.Build(options);

        // Привязка Ctrl+C к отмене.
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        IConsoleUI ui = provider.GetRequiredService<IConsoleUI>();
        IProfilerOrchestrator orchestrator = provider.GetRequiredService<IProfilerOrchestrator>();
        IProfilerHttpServer httpServer = provider.GetRequiredService<IProfilerHttpServer>();

        PrintBanner(ui);
        ui.Info($"Trace profile: {options.TraceProfile}, duration: {options.TraceDuration}с, " +
                $"output: {options.OutputDir}");

        // Встроенный health-сервер профайлера (http://localhost:{HttpPort}/health).
        await httpServer.StartAsync(cts.Token);

        try
        {
            int exitCode = await orchestrator.RunAllAsync(cts.Token);
            return exitCode;
        }
        catch (OperationCanceledException)
        {
            ui.Warn("Операция отменена пользователем.");
            return 1;
        }
        catch (Exception ex)
        {
            ui.Error($"Непредвиденная ошибка: {ex.Message}");
            ui.Detail(ex.ToString());
            return 1;
        }
        finally
        {
            // Гарантированная остановка сервера, включая Ctrl+C.
            await httpServer.StopAsync(CancellationToken.None);
        }
    }

    private static void PrintBanner(IConsoleUI ui)
    {
        ui.SectionHeader("MarketDataCollector Profiler");
    }
}
