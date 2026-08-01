using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Core.ProcessIdSources;
using MarketDataCollector.Profiler.Core.Waiters;
using MarketDataCollector.Profiler.Options;
using MarketDataCollector.Profiler.Reporting;
using MarketDataCollector.Profiler.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Cli;

/// <summary>Сборка контейнера зависимостей и настройка HttpClient.</summary>
public static class DiContainer
{
    public static ServiceProvider Build(ProfilerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        ServiceCollection services = new();

        // Логирование в консоль, одна строка, минимальный уровень Debug.
        services.AddLogging(builder =>
        {
            builder.ClearProviders();
            builder.AddSimpleConsole(c =>
            {
                c.SingleLine = true;
                c.TimestampFormat = "HH:mm:ss ";
            });
            builder.SetMinimumLevel(LogLevel.Debug);
        });

        // Опции.
        services.AddSingleton(options);

        // HTTP-логгер (DelegatingHandler) для именованных клиентов.
        services.AddTransient<HttpLoggingHandler>();

        // Именованные HttpClient с таймаутами.
        services.AddHttpClient("Metrics", client => client.Timeout = TimeSpan.FromSeconds(10))
            .AddHttpMessageHandler<HttpLoggingHandler>();

        services.AddHttpClient("Health", client => client.Timeout = TimeSpan.FromSeconds(3))
            .AddHttpMessageHandler<HttpLoggingHandler>();

        // Базовые компоненты.
        services.AddSingleton<IConsoleUI, ConsoleUI>();
        services.AddSingleton<IToolRunner, ToolRunner>();
        services.AddSingleton<IEnsureDotnetTools, EnsureDotnetTools>();
        services.AddSingleton<IPrometheusParser, PrometheusParser>();

        // Поиск процесса (стратегии).
        services.AddSingleton<IProcessIdSource>(sp => new TracePsSource(
            sp.GetRequiredService<IToolRunner>(),
            sp.GetRequiredService<ILogger<TracePsSource>>(),
            options.WorkerProcessName));
        services.AddSingleton<IProcessIdSource>(sp => new TargetProcessSource(
            sp.GetRequiredService<ILogger<TargetProcessSource>>(),
            options.WorkerProcessName));
        // WmiProcessSource доступен только на Windows.
        services.AddSingleton<IProcessIdSource>(sp =>
            OperatingSystem.IsWindows()
                ? new WmiProcessSource(sp.GetRequiredService<ILogger<WmiProcessSource>>(), options.WorkerProcessName)
                : new NullProcessSource());
        services.AddSingleton<IProcessFinder, ProcessFinder>();

        // Сборщики и утилиты.
        services.AddSingleton<ITraceCollector, TraceCollector>();
        services.AddSingleton<IGcDumpCollector, GcDumpCollector>();
        services.AddSingleton<ISpeedScopeConverter, SpeedScopeConverter>();
        services.AddSingleton<IPeakLoadWaiter, PeakLoadWaiter>();
        services.AddSingleton<IDrainWaiter, DrainWaiter>();
        services.AddSingleton<IHealthCheckService, HealthCheckService>();
        services.AddSingleton<ICountersCollector, CountersCollector>();
        services.AddSingleton<IReportGenerator, ReportGenerator>();
        services.AddSingleton<IProfilerOrchestrator, ProfilerOrchestrator>();

        return services.BuildServiceProvider();
    }
}
