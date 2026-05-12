using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Interfaces;
using MarketDataCollector.Infrastructure.Data;
using MarketDataCollector.Infrastructure.Factories;
using MarketDataCollector.Infrastructure.Repositories;
using MarketDataCollector.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

// Database
builder.Services.AddDbContext<MarketDataDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("MarketDataDb")));

// Configuration
builder.Services.Configure<ExchangeOptions>(builder.Configuration.GetSection(ExchangeOptions.SectionName));
builder.Services.Configure<MarketDataProcessorOptions>(builder.Configuration.GetSection(MarketDataProcessorOptions.SectionName));

// Core services
builder.Services.AddScoped<IMarketDataProcessor>(sp =>
{
    var repository = sp.GetRequiredService<IRawTickRepository>();
    var logger = sp.GetRequiredService<ILogger<MarketDataProcessor>>();
    var timeService = sp.GetRequiredService<ITimeService>();
    var options = sp.GetRequiredService<IOptions<MarketDataProcessorOptions>>().Value;
    
    return new MarketDataProcessor(
        repository,
        logger,
        timeService,
        options.BatchSize,
        options.ChannelCapacity
    );
});
builder.Services.AddScoped<IRawTickRepository, RawTickRepository>();
builder.Services.AddScoped<IConnectionLogRepository, ConnectionLogRepository>();
builder.Services.AddSingleton<ITimeService, SystemTimeService>();

// Monitoring service — singleton, т.к. хранит состояние всех клиентов
builder.Services.AddSingleton<IMonitoringService, MonitoringService>();

// WebSocket client factory (must be scoped because it depends on scoped IMarketDataProcessor)
builder.Services.AddScoped<IWebSocketClientFactory, WebSocketClientFactory>();

// Worker
builder.Services.AddHostedService<MarketDataCollector.Worker.Worker>();

var host = builder.Build();
host.Run();
