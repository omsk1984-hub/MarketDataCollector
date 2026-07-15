using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Data;
using MarketDataCollector.Infrastructure.Factories;
using MarketDataCollector.Infrastructure.Kafka;
using MarketDataCollector.Infrastructure.Repositories;
using MarketDataCollector.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);

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

var host = builder.Build();
host.Run();
