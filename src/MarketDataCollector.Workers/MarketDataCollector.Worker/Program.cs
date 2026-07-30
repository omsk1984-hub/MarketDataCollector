using System.Runtime;
using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Data;
using MarketDataCollector.Infrastructure.Factories;
using Confluent.Kafka;
using MarketDataCollector.Infrastructure.Kafka;
using MarketDataCollector.Infrastructure.Repositories;
using MarketDataCollector.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.AspNetCore.Builder;
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

// Database
builder.Services.AddDbContext<MarketDataDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("MarketDataDb")));

// Configuration
builder.Services.Configure<ExchangeOptions>(builder.Configuration.GetSection(ExchangeOptions.SectionName));
builder.Services.Configure<MarketDataProcessorOptions>(builder.Configuration.GetSection(MarketDataProcessorOptions.SectionName));
builder.Services.Configure<TickAggregatorOptions>(builder.Configuration.GetSection(TickAggregatorOptions.SectionName));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));

// ===== Kafka Integration =====
var kafkaConfig = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>();
if (kafkaConfig?.Enabled == true)
{
    // Проверка доступности Kafka при старте
    try
    {
        var testConfig = new Confluent.Kafka.AdminClientConfig { BootstrapServers = kafkaConfig.BootstrapServers };
        using var adminClient = new Confluent.Kafka.AdminClientBuilder(testConfig).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
        if (metadata.Brokers.Count > 0)
        {
            Console.WriteLine($"[Kafka] Broker(s) available: {string.Join(", ", metadata.Brokers.Select(b => $"{b.Host}:{b.Port}"))}");
        }
        else
        {
            Console.WriteLine($"[Kafka] WARNING: No brokers found at {kafkaConfig.BootstrapServers}. Candles will fall back to direct DB write until Kafka becomes available.");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Kafka] WARNING: Cannot reach Kafka at {kafkaConfig.BootstrapServers}: {ex.Message}. Candles will fall back to direct DB write until Kafka becomes available.");
    }

    // Kafka producer (singleton — пул соединений)
    builder.Services.AddSingleton<IKafkaProducer<string, string>>(sp =>
    {
        var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var logger = sp.GetRequiredService<ILogger<KafkaProducer>>();
        return new KafkaProducer(options, logger);
    });

    // Kafka candle producer (singleton — обёртка над IKafkaProducer)
    builder.Services.AddSingleton<KafkaCandleProducer>();

    // Kafka candle consumer (hosted service — читает свечи из Kafka и пишет в БД)
    builder.Services.AddHostedService<KafkaCandleConsumerService>();

    // Aggregation service with Kafka (singleton, because it maintains in-memory state)
    builder.Services.AddSingleton<ITickAggregator>(sp =>
    {
        var timeService = sp.GetRequiredService<ITimeService>();
        var logger = sp.GetRequiredService<ILogger<TickAggregator>>();
        var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
        var options = sp.GetRequiredService<IOptions<TickAggregatorOptions>>();
        var kafkaCandleProducer = sp.GetRequiredService<KafkaCandleProducer>();
        var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>();

        return new TickAggregator(timeService, logger, scopeFactory, options,
            kafkaCandleProducer, kafkaOptions);
    });
}
else
{
    // Kafka отключена — TickAggregator пишет напрямую в БД (как сейчас)
    builder.Services.AddSingleton<ITickAggregator, TickAggregator>();
}
// ===== End Kafka Integration =====

// Core services
// MarketDataProcessor — Singleton, так как является единой точкой входа для всех
// WebSocket-клиентов. При этом каждый batch создаёт отдельный scope для DbContext
// через IServiceScopeFactory, что гарантирует thread-safe работу.
builder.Services.AddSingleton<IMarketDataProcessor>(sp =>
{
    var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
    var logger = sp.GetRequiredService<ILogger<MarketDataProcessor>>();
    var timeService = sp.GetRequiredService<ITimeService>();
    var options = sp.GetRequiredService<IOptions<MarketDataProcessorOptions>>().Value;
    var tickAggregator = sp.GetService<ITickAggregator>();
    
    return new MarketDataProcessor(
        scopeFactory,
        logger,
        timeService,
        options,
        tickAggregator
    );
});
builder.Services.AddScoped<IRawTickRepository, RawTickRepository>();
builder.Services.AddScoped<IConnectionLogRepository, ConnectionLogRepository>();
builder.Services.AddScoped<IAggregatedDataRepository, AggregatedDataRepository>();
builder.Services.AddSingleton<ITimeService, SystemTimeService>();

// Monitoring service — singleton, т.к. хранит состояние всех клиентов
builder.Services.AddSingleton<IMonitoringService, MonitoringService>();

// WebSocket client factory — Scoped, т.к. создаёт клиенты внутри одного scope Worker'а.
// Зависимость от IMarketDataProcessor (Singleton) разрешается корректно.
builder.Services.AddScoped<IWebSocketClientFactory, WebSocketClientFactory>();

// Worker
builder.Services.AddHostedService<MarketDataCollector.Worker.Worker>();

var app = builder.Build();

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

    var allHealthy = healthChecks.Values.All(h =>
    {
        var status = h.GetType().GetProperty("status")?.GetValue(h)?.ToString();
        return status == "healthy" || status == "disabled";
    });

    ctx.Response.StatusCode = allHealthy ? 200 : 503;
    await ctx.Response.WriteAsJsonAsync(new
    {
        status = allHealthy ? "healthy" : "degraded",
        checks = healthChecks,
        timestamp = DateTime.UtcNow
    });
});

// ===== Prometheus scrape endpoint =====
app.MapPrometheusScrapingEndpoint("/metrics");

app.Run();
