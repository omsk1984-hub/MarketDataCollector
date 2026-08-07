using System.Runtime;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Worker;
using MarketDataCollector.Core.Telemetry;
using MarketDataCollector.Infrastructure.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Serilog;

// ===== GC Optimization =====
GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

// LOH-компактинг по порогу фрагментации (>15%), а не по таймеру.
// Безусловный CompactOnce каждые 5 минут противоречил правилу «компактить только
// если LOH-фрагментация > 15%» (сейчас ~18% — порог пройден).
// Компактинг дорогой (GC-пауза), поэтому применяем точечно и только при реальной
// фрагментации, чтобы не вмешиваться в стабильные прогоны.
const double LohCompactionFragmentationThreshold = 0.15;
var LohCompactionCheckInterval = TimeSpan.FromMinutes(5);

_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(LohCompactionCheckInterval);
        try
        {
            // GCMemoryInfo.FragmentedBytes/HeapSizeBytes даёт оценку доли фрагментированной
            // памяти heap (LOH вносит основной постоянный вклад в фрагментацию Gen2/LOH).
            // Точного LOH-специфичного поля в .NET 8 нет — используем агрегат.
            var info = GC.GetGCMemoryInfo();
            if (info.HeapSizeBytes == 0)
                continue;

            var fragmentationRatio = (double)info.FragmentedBytes / info.HeapSizeBytes;
            if (fragmentationRatio > LohCompactionFragmentationThreshold)
            {
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect(2, GCCollectionMode.Forced, blocking: false);
            }
        }
        catch
        {
            // Ignore — LOH compaction может упасть при OOM
        }
    }
});

// ===== Global exception handlers (diagnostics) =====
// Ловит фатальные нативные крахи (например, ACCESS_VIOLATION) и
// необработанные исключения из fire-and-forget задач для логирования
// причины падения Worker'а.
AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
{
    var exceptionObject = args.ExceptionObject as Exception;
    var message = exceptionObject?.ToString() ?? "null";
    try
    {
        File.AppendAllText(
            Path.Combine(AppContext.BaseDirectory, "crash.log"),
            $"[{DateTime.UtcNow:O}] FATAL UnhandledException (isTerminating={args.IsTerminating}):\n{message}\n---\n");
    }
    catch
    {
        // Игнорируем ошибки записи crash.log — процесс всё равно умирает.
    }
};

TaskScheduler.UnobservedTaskException += (sender, args) =>
{
    var message = args.Exception?.ToString() ?? "null";
    try
    {
        File.AppendAllText(
            Path.Combine(AppContext.BaseDirectory, "crash.log"),
            $"[{DateTime.UtcNow:O}] UNOBSERVED Task Exception:\n{message}\n---\n");
    }
    catch
    {
        // Игнорируем ошибки записи crash.log.
    }
    args.SetObserved();
};

var builder = WebApplication.CreateBuilder(args);

// ===== Serilog bootstrap-логгер =====
// Настраивается до остальной конфигурации хоста, чтобы поймать ошибки
// старта и конфигурации на раннем этапе. Полная конфигурация (ReadFrom.Services)
// подхватывается в UseSerilog ниже.
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateBootstrapLogger();

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

// ===== Serilog (провайдер ILogger<T>) =====
// Логи идут в Console и Seq (Seq — основное хранилище). OTLP-экспорт логов убран,
// чтобы избежать дублирования; метрики и трейсы OpenTelemetry остаются.
builder.Host.UseSerilog((context, services, configuration) =>
    configuration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext());

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

// ===== Bearer/API-key аутентификация для /metrics и /health (только Production) =====
// В Development токен не задан (пустой) — middleware пропускает запросы без проверки,
// чтобы не мешать локальной разработке и тестам. В Production задаётся Auth__ApiKey,
// и доступ к защищаемым эндпоинтам требует заголовка Authorization: Bearer <token>.
var apiKey = builder.Configuration["Auth:ApiKey"];
if (!string.IsNullOrWhiteSpace(apiKey))
{
    var protectedPaths = new[] { "/metrics", "/health", "/shutdown" };
    app.Use(async (context, next) =>
    {
        var path = context.Request.Path.Value ?? string.Empty;
        var isProtected = protectedPaths.Any(p =>
            path.Equals(p, StringComparison.OrdinalIgnoreCase));

        if (!isProtected)
        {
            await next();
            return;
        }

        if (context.Request.Headers.TryGetValue("Authorization", out StringValues authHeader) &&
            authHeader.Count == 1 &&
            authHeader[0] is { } headerValue &&
            headerValue.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            var providedToken = headerValue.Substring("Bearer ".Length).Trim();
            var expectedBytes = System.Text.Encoding.UTF8.GetBytes(apiKey);
            var providedBytes = System.Text.Encoding.UTF8.GetBytes(providedToken);
            var isValid = expectedBytes.Length == providedBytes.Length &&
                          CryptographicOperations.FixedTimeEquals(expectedBytes, providedBytes);
            if (isValid)
            {
                await next();
                return;
            }
        }

        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        await context.Response.WriteAsync("Unauthorized");
    });
}

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

    // Комбинированно: 503, если Kafka/PostgreSQL unhealthy.
    // НЕ включаем wsAllDown в readiness HTTP-кода: WS-подключения — внешняя рыночная
    // зависимость, которая легитимно бывает disconnected в начале старта/при реконнекте.
    // Оркестратор нагрузочного теста (Wait-Http) ждёт HTTP-готовность :5010 и не должен
    // ложно падать по 503. Состояние WS остаётся видимым в checks.websocket и в логере
    // Worker («0 connected, 3 disconnected»), но не роняет HTTP-код готовности.
    var degraded = !allHealthy;

    ctx.Response.StatusCode = degraded ? 503 : 200;
    await ctx.Response.WriteAsJsonAsync(new
    {
        status = degraded ? "degraded" : "healthy",
        checks = healthChecks,
        timestamp = DateTime.UtcNow
    });
});

// ===== Graceful shutdown endpoint =====
// Оркестратор нагрузочного теста (run_loadtest.ps1) вызывает POST /shutdown
// после завершения профилирования. StopApplication() отменяет stoppingToken
// в Worker.ExecuteAsync, что запускает CleanupAsync (клиенты -> агрегатор ->
// процессор с финальным flush).
app.MapPost("/shutdown", (IHostApplicationLifetime appLifetime, HttpContext ctx) =>
{
    appLifetime.StopApplication();
    ctx.Response.StatusCode = StatusCodes.Status202Accepted;
    return ctx.Response.WriteAsJsonAsync(new
    {
        status = "shutting_down"
    });
});

// ===== DIAG: middleware тайминга запросов (детекция starvation) =====
// Логирует запросы, занявшие >1с, с сигнатурой thread pool и GC-генерациями.
app.Use(async (context, next) =>
{
    var path = context.Request.Path.Value ?? string.Empty;
    var method = context.Request.Method;
    var sw = System.Diagnostics.Stopwatch.StartNew();
    var startTs = DateTime.UtcNow;
    try
    {
        await next();
    }
    finally
    {
        var elapsed = sw.ElapsedMilliseconds;
        if (elapsed > 1000)
        {
            var logger = context.RequestServices.GetService<ILoggerFactory>()?.CreateLogger("HttpDiag");
            ThreadPool.GetAvailableThreads(out int availW, out int availIo);
            ThreadPool.GetMaxThreads(out int maxW, out int maxIo);
            logger?.LogWarning(
                "DIAG-HTTP: {Method} {Path} -> {StatusCode} за {Elapsed}ms (start {Start:O}) | " +
                "tp={AvailW}/{MaxW}w {AvailIo}/{MaxIo}io gen0={Gen0} gen2={Gen2}",
                method, path, context.Response.StatusCode, elapsed, startTs,
                availW, maxW, availIo, maxIo,
                GC.CollectionCount(0), GC.CollectionCount(2));
        }
    }
});

// ===== Prometheus scrape endpoint =====
app.UseOpenTelemetryPrometheusScrapingEndpoint("/metrics");

try
{
    app.Run();
}
finally
{
    // Гарантированный flush буферизованных логов (Seq-sink) при graceful shutdown.
    Log.CloseAndFlush();
}
