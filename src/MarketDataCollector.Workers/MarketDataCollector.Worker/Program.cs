using System.Runtime;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Worker;
using MarketDataCollector.Core.Telemetry;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

// ===== GC Optimization =====
GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

// Периодическая LOH compaction (каждые 5 минут) для снижения LOH фрагментации
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromMinutes(5));
        try
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Forced, blocking: false);
        }
        catch
        {
            // Ignore — LOH compaction может упасть при OOM
        }
    }
});

var builder = WebApplication.CreateBuilder(args);

// ===== OpenTelemetry Configuration =====
var otelOptions = builder.Configuration.GetSection("OpenTelemetry");
var otlpEndpoint = otelOptions["OtlpEndpoint"] ?? "http://localhost:4317";
var serviceName = otelOptions["ServiceName"] ?? "MarketDataCollector.Worker";

// ===== OpenTelemetry Metrics & Tracing =====
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(serviceName))
    .WithMetrics(metrics => metrics
        .AddRuntimeInstrumentation()
        .AddMeter(MarketDataCollector.Core.Telemetry.MarketDataTelemetry.MeterName)
        .AddPrometheusExporter()
        .AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint)))
    .WithTracing(tracing => tracing
        .AddEntityFrameworkCoreInstrumentation()
        .AddSource(MarketDataCollector.Core.Telemetry.MarketDataTelemetry.ActivitySourceName)
        .AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint)));

// ===== OpenTelemetry Logging =====
builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
    logging.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName));
    logging.AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint));
});

// ===== Configuration =====
builder.Services.AddConfiguration(builder.Configuration);

// ===== Persistence (DbContext + repositories) =====
builder.Services.AddPersistence(builder.Configuration);

// ===== Core services (time, monitoring, ws factory) =====
builder.Services.AddCoreServices();

// ===== Aggregation (Kafka + TickAggregator) =====
builder.Services.AddAggregation(builder.Configuration);

// ===== Main market data pipeline =====
builder.Services.AddMarketDataPipeline();

// ===== Worker =====
builder.Services.AddHostedService<MarketDataCollector.Worker.Worker>();

var app = builder.Build();

// ===== Авто-миграция схемы БД при старте =====
// Применяет все ожидающие EF Core миграции. Это упрощает деплой (не нужен
// отдельный шаг миграции), но требует, чтобы Postgres был доступен к этому
// моменту. При нескольких репликах возможна гонка — миграции EF Core
// идемпотентны по применённым шагам, повторное применение безопасно.
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<MarketDataDbContext>();
    db.Database.Migrate();
}

// Kafka config — для health check ниже
var kafkaConfig = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>();

// ===== Health check endpoint =====
app.MapGet("/health", async (HttpContext ctx) =>
{
    var healthChecks = new Dictionary<string, object>();

    // Kafka health check
    if (kafkaConfig?.Enabled == true)
    {
        try
        {
            var kafkaOptions = ctx.RequestServices.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var testConfig = new Confluent.Kafka.AdminClientConfig { BootstrapServers = kafkaOptions.BootstrapServers };
            using var adminClient = new Confluent.Kafka.AdminClientBuilder(testConfig).Build();
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
            healthChecks["kafka"] = new
            {
                status = metadata.Brokers.Count > 0 ? "healthy" : "unhealthy",
                brokers = metadata.Brokers.Count,
                bootstrapServers = kafkaOptions.BootstrapServers
            };
        }
        catch (Exception ex)
        {
            healthChecks["kafka"] = new
            {
                status = "unhealthy",
                error = ex.Message,
                bootstrapServers = kafkaConfig.BootstrapServers
            };
        }
    }
    else
    {
        healthChecks["kafka"] = new { status = "disabled" };
    }

    // PostgreSQL health check
    try
    {
        using var scope = ctx.RequestServices.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MarketDataDbContext>();
        var canConnect = await db.Database.CanConnectAsync();
        healthChecks["postgresql"] = new
        {
            status = canConnect ? "healthy" : "unhealthy"
        };
    }
    catch (Exception ex)
    {
        healthChecks["postgresql"] = new
        {
            status = "unhealthy",
            error = ex.Message
        };
    }

    // ===== WebSocket clients health =====
    var clientRegistry = ctx.RequestServices.GetRequiredService<IWebSocketClientRegistry>();
    var wsClients = clientRegistry.GetClients();
    var connectedClients = wsClients.Count(c => c.IsConnected);
    var wsInfo = new
    {
        // Если клиенты зарегистрированы, но все отключены — unhealthy.
        // Если реестр пуст (клиенты ещё не созданы или Worker остановлен) — unknown.
        status = wsClients.Count == 0 ? "unknown"
               : (connectedClients > 0 ? "healthy" : "unhealthy"),
        total = wsClients.Count,
        connected = connectedClients,
        disconnected = wsClients.Count - connectedClients,
        clients = wsClients.Select(c => new
        {
            exchange = c.ExchangeName,
            name = c.Name,
            symbol = c.Symbol,
            connected = c.IsConnected,
            messagesPerSecond = c.GetMessagesPerSecond(),
            totalMessages = c.GetTotalMessagesCount()
        })
    };
    healthChecks["websocket"] = wsInfo;

    // ===== Channels fill-level (informational only) =====
    var processor = ctx.RequestServices.GetRequiredService<IMarketDataProcessor>();
    var fillLevels = processor.GetChannelFillLevels();
    var totalCount = fillLevels.Sum(f => f.Count);
    var totalCapacity = fillLevels.Sum(f => f.Capacity);
    var channelsInfo = new
    {
        channels = fillLevels.Select(f => new
        {
            count = f.Count,
            capacity = f.Capacity,
            fillPercent = f.Capacity > 0 ? Math.Round((double)f.Count / f.Capacity * 100.0, 1) : 0.0
        }),
        totalCount,
        totalCapacity,
        totalFillPercent = totalCapacity > 0 ? Math.Round((double)totalCount / totalCapacity * 100.0, 1) : 0.0,
        estimatedDropped = processor.GetEstimatedDroppedCount(),
        incoming = processor.GetTotalIncomingCount(),
        received = processor.GetTotalReceivedCount(),
        processedRps = processor.GetProcessedRps()
    };
    healthChecks["channels"] = channelsInfo;

    var allHealthy = healthChecks.Values.All(h =>
    {
        var status = h.GetType().GetProperty("status")?.GetValue(h)?.ToString();
        // null — информационные блоки без статуса (например, channels) не влияют на здоровье.
        return status == null || status == "healthy" || status == "disabled" || status == "unknown";
    });

    // Комбинированно: 503, если Kafka/PostgreSQL unhealthy ИЛИ все WS-клиенты отключены.
    var wsAllDown = wsClients.Count > 0 && connectedClients == 0;
    var degraded = !allHealthy || wsAllDown;

    ctx.Response.StatusCode = degraded ? 503 : 200;
    await ctx.Response.WriteAsJsonAsync(new
    {
        status = degraded ? "degraded" : "healthy",
        checks = healthChecks,
        timestamp = DateTime.UtcNow
    });
});

// ===== Prometheus scrape endpoint =====
app.MapPrometheusScrapingEndpoint("/metrics");

app.Run();
