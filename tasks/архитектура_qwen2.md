# Архитектура системы сбора рыночных данных (C# / .NET 8)

## Краткая рекомендация

Для нагрузки 50-100 тиков/сек с 2-3 источниками рекомендуется одно консольное приложение.

Сравнение подходов:

Критерий               | Одно приложение      | Несколько приложений
-----------------------|----------------------|------------------------
Сложность разработки   | Низкая               | Высокая
Отладка                | Один лог             | Агрегация логов
Ресурсы                | Минимум              | Избыточно
Надёжность             | Падение всех         | Изоляция сбоев
Для 50-100 тиков/сек   | Идеально             | Overengineering

Правило: Начинайте с монолита. Разделяйте только при упоре в лимиты CPU/RAM.

## Структура проекта (Clean Architecture)

CryptoDataCollector/
    src/
        Core/
            Entities/
                Tick.cs
                ExchangeType.cs
            Interfaces/
                IExchangeClient.cs
                IDataRepository.cs
                IMonitoringService.cs
        Infrastructure/
            Exchanges/
                KrakenClient.cs
                BinanceClient.cs
            Database/
                PostgresRepository.cs
            WebSocket/
                ResilientWebSocketClient.cs
        Application/
            Services/
                DataNormalizationService.cs
                DeduplicationService.cs
                MonitoringService.cs
            Pipelines/
                TickProcessingPipeline.cs
        Host/
            Program.cs
            DataCollectorWorker.cs
            appsettings.json
    tests/
    docker-compose.yml

## Ключевые компоненты

### 1. Core/Entities/Tick.cs

public record Tick(
    string Symbol,
    decimal Price,
    decimal Volume,
    DateTimeOffset Timestamp,
    ExchangeType Source,
    string RawId
);

public enum ExchangeType { Kraken, Binance, Bybit }

### 2. Core/Interfaces/IExchangeClient.cs

public interface IExchangeClient : IAsyncDisposable
{
    string Name { get; }
    event Func<Tick, Task> OnTickReceived;
    event Func<string, Task> OnLog;
    Task ConnectAsync(CancellationToken ct);
}

### 3. Infrastructure/WebSocket/ResilientWebSocketClient.cs

public abstract class ResilientWebSocketClient
{
    private ClientWebSocket? _ws;
    private CancellationTokenSource? _cts;
    
    protected abstract string Url { get; }
    protected abstract Task OnMessageReceived(string message, CancellationToken ct);
    protected abstract Task OnConnected(CancellationToken ct);

    public async Task StartAsync(CancellationToken ct)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                _ws = new ClientWebSocket();
                await _ws.ConnectAsync(new Uri(Url), _cts.Token);
                await OnConnected(_cts.Token);
                await ReceiveLoop(_cts.Token);
            }
            catch (OperationCanceledException) when (_cts.Token.IsCancellationRequested)
            {
                // Корректное завершение
            }
            catch (Exception ex)
            {
                await LogAsync($"Ошибка: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token);
            }
            finally
            {
                await CleanupAsync();
            }
        }
    }

    private async Task ReceiveLoop(CancellationToken ct)
    {
        var buffer = new byte[8192];
        while (!ct.IsCancellationRequested && _ws?.State == WebSocketState.Open)
        {
            var result = await _ws.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
            if (result.MessageType == WebSocketMessageType.Text)
            {
                var msg = Encoding.UTF8.GetString(buffer, 0, result.Count);
                await OnMessageReceived(msg, ct);
            }
        }
    }

    protected async Task LogAsync(string message)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {message}");
    }

    private async Task CleanupAsync()
    {
        if (_ws?.State == WebSocketState.Open)
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Close", CancellationToken.None);
        _ws?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();
        await CleanupAsync();
        _cts?.Dispose();
    }
}

### 4. Infrastructure/Exchanges/KrakenClient.cs

public class KrakenClient : ResilientWebSocketClient, IExchangeClient
{
    public string Name => "Kraken";
    protected override string Url => "wss://ws.kraken.com";
    public event Func<Tick, Task>? OnTickReceived;
    public event Func<string, Task>? OnLog;

    protected override async Task OnConnected(CancellationToken ct)
    {
        var subscribeMessage = new 
        { 
            @event = "subscribe", 
            pair = new[] { "XBT/USD", "ETH/USD" }, 
            subscription = new { name = "trade" } 
        };
        await SendAsync(JsonSerializer.Serialize(subscribeMessage), ct);
        await (OnLog?.Invoke("Kraken: subscribed") ?? Task.CompletedTask);
    }

    protected override async Task OnMessageReceived(string message, CancellationToken ct)
    {
        if (JsonDocument.TryParse(message, out var doc) && 
            doc.RootElement.ValueKind == JsonValueKind.Array && 
            doc.RootElement.GetArrayLength() >= 4)
        {
            var pair = doc.RootElement[3].GetString();
            foreach (var trade in doc.RootElement[1].EnumerateArray())
            {
                var tick = new Tick(
                    Symbol: pair == "XBT/USD" ? "BTC/USD" : pair!,
                    Price: trade[0].GetDecimal(),
                    Volume: trade[1].GetDecimal(),
                    Timestamp: DateTimeOffset.FromUnixTimeSeconds((long)trade[2].GetDouble()),
                    Source: ExchangeType.Kraken,
                    RawId: $"{pair}_{trade[2]}"
                );
                if (OnTickReceived != null)
                    await OnTickReceived(tick);
            }
        }
    }

    private async Task SendAsync(string json, CancellationToken ct)
    {
        await _ws!.SendAsync(
            new ArraySegment<byte>(Encoding.UTF8.GetBytes(json)), 
            WebSocketMessageType.Text, 
            true, 
            ct);
    }
}

### 5. Application/Pipelines/TickProcessingPipeline.cs

public class TickProcessingPipeline : BackgroundService
{
    private readonly Channel<Tick> _channel;
    private readonly IDataRepository _repository;
    private readonly IMonitoringService _monitoring;
    private readonly HashSet<string> _recentIds = new();
    private readonly TimeSpan _dedupWindow = TimeSpan.FromSeconds(2);

    public TickProcessingPipeline(IDataRepository repository, IMonitoringService monitoring)
    {
        _channel = Channel.CreateBounded<Tick>(new BoundedChannelOptions(10000) 
        { 
            SingleWriter = false, 
            SingleReader = true 
        });
        _repository = repository;
        _monitoring = monitoring;
    }

    public async Task SendAsync(Tick tick, CancellationToken ct)
    {
        await _channel.Writer.WriteAsync(tick, ct);
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await foreach (var tick in _channel.Reader.ReadAllAsync(ct))
        {
            // Deduplication
            if (_recentIds.Contains(tick.RawId))
                continue;
                
            _recentIds.Add(tick.RawId);
            _ = Task.Delay(_dedupWindow, ct).ContinueWith(_ => 
                _recentIds.Remove(tick.RawId), ct);

            // Save to DB
            await _repository.SaveAsync(tick, ct);
            
            // Monitoring
            _monitoring.Increment();
        }
    }
}

### 6. Host/Program.cs

var builder = Host.CreateApplicationBuilder(args);

// Services
builder.Services.AddSingleton<TickProcessingPipeline>();
builder.Services.AddSingleton<IDataRepository, PostgresRepository>();
builder.Services.AddSingleton<IMonitoringService, ConsoleMonitoringService>();

// Exchange clients
builder.Services.AddSingleton<IExchangeClient, KrakenClient>();
// builder.Services.AddSingleton<IExchangeClient, BinanceClient>();

// Worker
builder.Services.AddHostedService<DataCollectorWorker>();

await builder.Build().RunAsync();

### 7. Host/DataCollectorWorker.cs

public class DataCollectorWorker : BackgroundService
{
    private readonly IEnumerable<IExchangeClient> _clients;
    private readonly TickProcessingPipeline _pipeline;
    private readonly ILogger<DataCollectorWorker> _logger;

    public DataCollectorWorker(
        IEnumerable<IExchangeClient> clients, 
        TickProcessingPipeline pipeline, 
        ILogger<DataCollectorWorker> logger)
    {
        _clients = clients;
        _pipeline = pipeline;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        foreach (var client in _clients)
        {
            client.OnTickReceived += _pipeline.SendAsync;
            client.OnLog += message => 
            { 
                _logger.LogInformation("{Client}: {Message}", client.Name, message); 
                return Task.CompletedTask; 
            };
            
            _ = client.ConnectAsync(ct);
            _logger.LogInformation("Started: {Client}", client.Name);
        }
        
        await Task.Delay(Timeout.Infinite, ct);
    }

    public override async Task StopAsync(CancellationToken ct)
    {
        foreach (var client in _clients)
            await client.DisposeAsync();
        await base.StopAsync(ct);
    }
}

## Когда переходить на микросервисы?

Признаки для разделения:

- Нагрузка > 1000 тиков/сек на источник
  Решение: Выделить источник в отдельный процесс

- Нужна независимая масштабируемость
  Решение: Kubernetes: 1 pod на биржу

- Разные SLA/приоритеты
  Решение: Отдельные процессы с разным priority

- Командная разработка
  Решение: Разные репозитории/команды

- Гео-распределение
  Решение: Сбор ближе к бирже, агрегация в ЦОД

Архитектура будущего:

    [Collector 1] ----+
    [Collector 2] ----+---> RabbitMQ/Kafka (topic: ticks.raw) ---> [Aggregator/DB Writer]
    [Collector 3] ----+

## Рекомендуемый стек

Компонент           Выбор                           Почему
-------------------|-------------------------------|---------------------------
Фреймворк          | .NET 8 Worker Service        | Встроенный DI, graceful shutdown
WebSocket          | ClientWebSocket + Polly      | Нативный, надёжный
Очередь в памяти   | System.Threading.Channels    | Высокая скорость, zero-dependency
База данных        | PostgreSQL + TimescaleDB     | Оптимизация под временные ряды
Доступ к БД        | Dapper (write) + EF Core     | Скорость + удобство миграций
Логирование        | Serilog                      | Структурированные логи
Мониторинг         | Console -> Prometheus        | Начинаем просто, масштабируем

## Быстрый старт (CLI команды)

dotnet new sln -n CryptoDataCollector
dotnet new classlib -n Core -f net8.0
dotnet new classlib -n Infrastructure -f net8.0
dotnet new classlib -n Application -f net8.0
dotnet new worker -n Host -f net8.0

dotnet sln add Core Infrastructure Application Host
dotnet add Infrastructure reference Core
dotnet add Application reference Core
dotnet add Host reference Application Infrastructure

dotnet add Host package Microsoft.Extensions.Hosting
dotnet add Infrastructure package System.Net.WebSockets.Client
dotnet add Host package Npgsql
dotnet add Host package Dapper
dotnet add Host package Serilog.AspNetCore

## Итог

Начните с одного приложения. Интерфейсы уже абстрагированы, поэтому при росте нагрузки вы легко вынесете сборщики в отдельные процессы, заменив Channel на брокер сообщений (RabbitMQ/Kafka).