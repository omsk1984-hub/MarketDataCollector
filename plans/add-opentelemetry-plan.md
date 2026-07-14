# План: Добавление OpenTelemetry в MarketDataCollector.Worker

## Цель
Добавить полноценный мониторинг (метрики + трейсинг + логи) через OpenTelemetry с **OTLP-экспортером**. Эндпоинт OTLP настраивается через секцию `OpenTelemetry` в `appsettings.json`.

---

## Шаг 1: Добавить NuGet-пакеты

В файл [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj) добавить:

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `OpenTelemetry` | `1.11.2` | Базовый SDK |
| `OpenTelemetry.Extensions.Hosting` | `1.11.2` | Интеграция с Hosting (даёт `AddOpenTelemetry()`) |
| `OpenTelemetry.Exporter.OpenTelemetryProtocol` | `1.11.2` | OTLP-экспортер (gRPC на порт 4317) |
| `OpenTelemetry.Instrumentation.Runtime` | `1.11.1` | Метрики .NET Runtime (GC, CPU, память) |
| `OpenTelemetry.Instrumentation.EntityFrameworkCore` | `1.0.0-beta.7` | Трейсинг EF Core запросов (опционально, можно убрать) |

> **Важно:** вместо `OpenTelemetry.Exporter.Console` используем `OpenTelemetry.Exporter.OpenTelemetryProtocol`.

---

## Шаг 2: Добавить секцию OpenTelemetry в appsettings.json

В файл [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) добавить:

```json
"OpenTelemetry": {
  "OtlpEndpoint": "http://localhost:4317",
  "ServiceName": "MarketDataCollector.Worker"
}
```

- `OtlpEndpoint` — адрес OTLP Collector (Jaeger/Grafana Tempo/Aspire Dashboard). По умолчанию gRPC на `localhost:4317`.
- `ServiceName` — имя сервиса для идентификации в трейсинге.

---

## Шаг 3: Настроить OpenTelemetry в Program.cs

В файл [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs) внести изменения:

### 3.1. Добавить using-директивы

```csharp
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
```

### 3.2. Прочитать конфигурацию OpenTelemetry

Сразу после `var builder = Host.CreateApplicationBuilder(args);`:

```csharp
var otelOptions = builder.Configuration.GetSection("OpenTelemetry");
var otlpEndpoint = otelOptions["OtlpEndpoint"] ?? "http://localhost:4317";
var serviceName = otelOptions["ServiceName"] ?? "MarketDataCollector.Worker";
```

### 3.3. Настроить Metrics + Tracing с OTLP

Добавить после конфигурации сервисов (но до `builder.Build()`):

```csharp
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(serviceName))
    .WithMetrics(metrics => metrics
        .AddRuntimeInstrumentation()
        .AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint)))
    .WithTracing(tracing => tracing
        .AddEntityFrameworkCoreInstrumentation()
        .AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint)));
```

### 3.4. Настроить Logging с OTLP

После `builder.Build()` (или можно до, через `builder.Logging`):

```csharp
builder.Logging.AddOpenTelemetry(logging =>
{
    logging.IncludeFormattedMessage = true;
    logging.IncludeScopes = true;
    logging.SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName));
    logging.AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint));
});
```

> **Примечание:** `builder.Logging.AddOpenTelemetry()` НЕ требует вызова `builder.Build()` — это расширение для `ILoggingBuilder`. Его можно и нужно вызывать **до** `builder.Build()`, сразу после настройки сервисов. Исправлю это в итоговом коде.

### 3.5. Итоговая структура Program.cs

```csharp
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
        .AddOtlpExporter(options => options.Endpoint = new Uri(otlpEndpoint)))
    .WithTracing(tracing => tracing
        .AddEntityFrameworkCoreInstrumentation()
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
    // Kafka producer
    builder.Services.AddSingleton<IKafkaProducer<string, string>>(sp =>
    {
        var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var logger = sp.GetRequiredService<ILogger<KafkaProducer>>();
        return new KafkaProducer(options, logger);
    });

    // Kafka candle producer
    builder.Services.AddSingleton<KafkaCandleProducer>();

    // Kafka candle consumer
    builder.Services.AddHostedService<KafkaCandleConsumerService>();

    // Aggregation with Kafka
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
    // Kafka off — TickAggregator writes directly to DB
    builder.Services.AddSingleton<ITickAggregator, TickAggregator>();
}

// Core services
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

// Monitoring
builder.Services.AddSingleton<IMonitoringService, MonitoringService>();

// WebSocket client factory
builder.Services.AddScoped<IWebSocketClientFactory, WebSocketClientFactory>();

// Worker
builder.Services.AddHostedService<MarketDataCollector.Worker.Worker>();

var host = builder.Build();
host.Run();
```

---

## Шаг 4: Проверить сборку

После внесения изменений выполнить:

```powershell
dotnet build src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj
```

Убедиться, что нет ошибок несовместимости версий пакетов.

---

## Примечания

1. **Версии пакетов** — актуальные на июль 2026. Если сборка упадёт, можно откатиться на `1.10.0` стабильную.
2. **EntityFrameworkCore Instrumentation** — добавляет трейсинг SQL-запросов. Если будет слишком шумно в Jaeger/Grafana, можно убрать `.AddEntityFrameworkCoreInstrumentation()`.
3. **OTLP-эндпоинт** — по умолчанию gRPC `localhost:4317`. Если нужен HTTP/protobuf, укажи `http://localhost:4318` в `appsettings.json`.
4. **Console Exporter** не используется — всё идёт через OTLP. Если нужен вывод и в консоль, можно добавить оба экспортёра одновременно.
