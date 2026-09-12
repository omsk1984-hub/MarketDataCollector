using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Services;

/// <summary>
/// Встроенный HTTP-сервер профайлера (Kestrel). Поднимается в <c>Program.cs</c>,
/// отдаёт собственный `/health` с JSON-статусом, `/status` с фазой сбора
/// и `/trigger-drained-collect` для сигнала двухфазного сбора.
/// </summary>
public sealed class ProfilerHttpServer : IProfilerHttpServer
{
    private readonly IProfilerMetricsRegistry _metrics;
    private readonly IProfilerOrchestrator _orchestrator;
    private readonly ProfilerOptions _options;
    private readonly ILogger<ProfilerHttpServer> _logger;
    private WebApplication? _app;

    public ProfilerHttpServer(
        IProfilerMetricsRegistry metrics,
        IProfilerOrchestrator orchestrator,
        ProfilerOptions options,
        ILogger<ProfilerHttpServer> logger)
    {
        _metrics = metrics;
        _orchestrator = orchestrator;
        _options = options;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(_metrics);

        if (!_options.HttpEnabled)
        {
            _logger.LogInformation("Встроенный HTTP-сервер отключён (--http-enabled=false).");
            return;
        }

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddConsoleFormatter<PlainConsoleFormatter, PlainConsoleFormatterOptions>();
        builder.Logging.AddConsole(c => c.FormatterName = nameof(PlainConsoleFormatter));
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(_options.HttpPort);
        });

        builder.Services.AddSingleton(_metrics);
        builder.Services.AddSingleton(_orchestrator);

        // Подавляем стандартный startup-лог "Now listening on" — выводим свой.
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

        WebApplication app = builder.Build();

        app.MapGet("/health", (IProfilerMetricsRegistry metrics) =>
        {
            ProfilerMetricsSnapshot snapshot = metrics.GetSnapshot();
            ProfilerHealthResponse response = new(
                Status: snapshot.Status,
                CurrentStep: snapshot.CurrentStep,
                Metrics: new ProfilerMetricsPayload(
                    TraceDurationSeconds: snapshot.TraceDurationSeconds,
                    GcDumpPeakSuccess: snapshot.GcDumpPeakSuccess,
                    GcDumpDrainedSuccess: snapshot.GcDumpDrainedSuccess,
                    CountersSamples: snapshot.CountersSamples,
                    SpeedScopeSuccess: snapshot.SpeedScopeSuccess,
                    ElapsedSeconds: snapshot.ElapsedSeconds),
                Timestamp: DateTime.UtcNow.ToString("O"));

            return snapshot.Status == "healthy"
                ? Results.Ok(response)
                : Results.Json(response, statusCode: StatusCodes.Status503ServiceUnavailable);
        });

        // Endpoint для сигнала от внешнего оркестратора (start_loadtest.ps1):
        // после завершения генерации нагрузки оркестратор вызывает POST /trigger-drained-collect,
        // и Profiler переходит к Phase 2 (drain + drained gcdump).
        app.MapPost("/trigger-drained-collect", (IProfilerOrchestrator orchestrator) =>
        {
            orchestrator.SignalDrainReady();
            return Results.Accepted("/health", new { status = "drain_signaled" });
        });

        // Endpoint для диагностики: показывает фазу сбора и состояние TCS сигнала.
        app.MapGet("/status", () =>
        {
            // Не имея прямого доступа к TCS, используем метрики из реестра.
            ProfilerMetricsSnapshot snapshot = _metrics.GetSnapshot();
            return Results.Json(new
            {
                phase = snapshot.CurrentStep switch
                {
                    "8.5. Ожидание сигнала дренажа" => "awaiting-drain-signal",
                    "9. Дренаж и второй gcdump" => "draining",
                    "Завершено" => "completed",
                    _ => "phase1-running"
                },
                currentStep = snapshot.CurrentStep,
                status = snapshot.Status
            });
        });

        _app = app;

        _logger.LogInformation(
            "Встроенный health-сервер профайлера: http://localhost:{Port}/health", _options.HttpPort);

        await app.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_app is null)
        {
            return;
        }

        try
        {
            await _app.StopAsync(cancellationToken);
        }
        finally
        {
            await _app.DisposeAsync();
            _app = null;
            _logger.LogInformation("Встроенный health-сервер профайлера остановлен.");
        }
    }
}
