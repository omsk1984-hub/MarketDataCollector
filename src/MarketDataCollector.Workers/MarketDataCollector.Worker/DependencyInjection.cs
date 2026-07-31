using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Clients;
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

namespace MarketDataCollector.Worker;

/// <summary>
/// Extension-методы для регистрации зависимостей по слоям.
/// Позволяет держать Program.cs тонким (только OpenTelemetry + вызовы регистраций).
/// </summary>
public static class DependencyInjection
{
    /// <summary>
    /// Регистрация DbContext и репозиториев (слой Infrastructure).
    /// </summary>
    public static IServiceCollection AddPersistence(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString("MarketDataDb")
            ?? throw new InvalidOperationException("Connection string 'MarketDataDb' is not configured.");

        services.AddDbContext<MarketDataDbContext>(options =>
            options.UseNpgsql(connectionString));

        services.AddScoped<IRawTickRepository, RawTickRepository>();
        services.AddScoped<IConnectionLogRepository, ConnectionLogRepository>();
        services.AddScoped<IAggregatedDataRepository, AggregatedDataRepository>();

        // Автоматическое обслуживание партиций rawticks (создание вперёд + retention).
        services.AddHostedService(sp => new PartitionMaintenanceService(
            sp.GetRequiredService<ILogger<PartitionMaintenanceService>>(),
            sp.GetRequiredService<IOptions<PartitioningOptions>>(),
            connectionString));

        return services;
    }

    /// <summary>
    /// Регистрация Options-классов конфигурации.
    /// </summary>
    public static IServiceCollection AddConfiguration(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ExchangeOptions>(configuration.GetSection(ExchangeOptions.SectionName));
        services.Configure<MarketDataProcessorOptions>(configuration.GetSection(MarketDataProcessorOptions.SectionName));
        services.Configure<TickAggregatorOptions>(configuration.GetSection(TickAggregatorOptions.SectionName));
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.Configure<PartitioningOptions>(configuration.GetSection(PartitioningOptions.SectionName));

        return services;
    }

    /// <summary>
    /// Регистрация базовых сервисов: время, мониторинг, WebSocket-фабрика.
    /// </summary>
    public static IServiceCollection AddCoreServices(this IServiceCollection services)
    {
        services.AddSingleton<ITimeService, SystemTimeService>();
        services.AddSingleton<IMonitoringService, MonitoringService>();
        services.AddSingleton<IWebSocketClientRegistry, WebSocketClientRegistry>();
        services.AddScoped<IWebSocketClientFactory, WebSocketClientFactory>();

        return services;
    }

    /// <summary>
    /// Регистрация основного пайплайна обработки рыночных данных.
    /// MarketDataProcessor — Singleton, т.к. единая точка входа для всех WebSocket-клиентов.
    /// Каждый batch создаёт отдельный scope через IServiceScopeFactory (thread-safe).
    /// </summary>
    public static IServiceCollection AddMarketDataPipeline(this IServiceCollection services)
    {
        services.AddSingleton<IMarketDataProcessor>(sp =>
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
                tickAggregator);
        });

        return services;
    }

    /// <summary>
    /// Регистрация агрегатора свечей. Зависит от KafkaOptions: если Kafka включена,
    /// TickAggregator конфигурируется с ICandlePublisher и пишет в Kafka; иначе — напрямую в БД.
    /// </summary>
    public static IServiceCollection AddAggregation(this IServiceCollection services, IConfiguration configuration)
    {
        var kafkaConfig = configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>();

        if (kafkaConfig?.Enabled == true)
        {
            // Проверка доступности Kafka при старте (логгер вместо Console.WriteLine).
            services.AddHostedService<KafkaAvailabilityCheckService>();

            // Базовый Kafka producer (singleton — пул соединений), используется
            // KafkaCandleProducer через ICandlePublisher.
            services.AddSingleton<IKafkaProducer<string, string>>(sp =>
                new KafkaProducer(
                    sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
                    sp.GetRequiredService<ILogger<KafkaProducer>>()));

            // Kafka candle producer — singleton (пул соединений), зарегистрирован как ICandlePublisher
            services.AddSingleton<ICandlePublisher, KafkaCandleProducer>();

            // Kafka candle consumer (hosted service — читает свечи из Kafka и пишет в БД)
            services.AddHostedService<KafkaCandleConsumerService>();

            // Aggregation service with Kafka (singleton, потому что хранит in-memory состояние)
            services.AddSingleton<ITickAggregator>(sp =>
            {
                var timeService = sp.GetRequiredService<ITimeService>();
                var logger = sp.GetRequiredService<ILogger<TickAggregator>>();
                var scopeFactory = sp.GetRequiredService<IServiceScopeFactory>();
                var options = sp.GetRequiredService<IOptions<TickAggregatorOptions>>();
                var candlePublisher = sp.GetRequiredService<ICandlePublisher>();
                var kafkaOptions = sp.GetRequiredService<IOptions<KafkaOptions>>();

                return new TickAggregator(timeService, logger, scopeFactory, options,
                    candlePublisher, kafkaOptions);
            });
        }
        else
        {
            // Kafka отключена — TickAggregator пишет напрямую в БД
            services.AddSingleton<ITickAggregator, TickAggregator>();
        }

        return services;
    }

}
