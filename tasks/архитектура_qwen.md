# ?? Архитектура системы сбора рыночных данных (C# / .NET 8)

## ?? Краткая рекомендация
Для нагрузки **50-100 тиков/сек с 2-3 источниками** однозначно рекомендуется **одно консольное приложение**.

| Критерий | Одно приложение | Несколько приложений |
|----------|----------------|---------------------|
| Сложность разработки | ? Низкая | ? Высокая (оркестрация, синхронизация) |
| Отладка | ? Один лог, один процесс | ? Нужно агрегировать логи из разных мест |
| Ресурсы | ? Минимум (общий пул потоков) | ? Избыточно (оверхед на каждый процесс) |
| Надёжность | ?? Падение = остановка всех источников | ? Изоляция сбоев |
| Для 50-100 тиков/сек | ? Идеально | ? Overengineering |

> ?? **Правило:** Начинайте с монолита. Разделяйте только при упоре в лимиты CPU/RAM или при необходимости независимого масштабирования.

## ?? Структура проекта (Clean Architecture)
CryptoDataCollector/
+-- src/
¦   +-- Core/
¦   ¦   +-- Entities/          # Tick, ExchangeType
¦   ¦   +-- Interfaces/        # IExchangeClient, IDataProcessor, IDataRepository
¦   ¦   L-- Events/            # Domain events
¦   ¦
¦   +-- Infrastructure/
¦   ¦   +-- Exchanges/         # KrakenClient, BinanceClient
¦   ¦   +-- Database/          # PostgresRepository (Dapper/EF)
¦   ¦   +-- WebSocket/         # ResilientWebSocketClient
¦   ¦   L-- Logging/           # Serilog конфигурация
¦   ¦
¦   +-- Application/
¦   ¦   +-- Services/
¦   ¦   ¦   +-- DataNormalizationService
¦   ¦   ¦   +-- DeduplicationService
¦   ¦   ¦   L-- MonitoringService
¦   ¦   L-- Pipelines/         # System.Threading.Channels
¦   ¦
¦   L-- Host/ (Console/Worker)
¦       +-- Program.cs         # DI, конфигурация
¦       +-- DataCollectorWorker.cs
¦       L-- appsettings.json
¦
-- tests/
L-- docker-compose.yml

# ?? Архитектура системы сбора рыночных данных (C# / .NET 8)

## ?? Краткая рекомендация
Для нагрузки **50-100 тиков/сек с 2-3 источниками** однозначно рекомендуется **одно консольное приложение**.

| Критерий | Одно приложение | Несколько приложений |
|----------|----------------|---------------------|
| Сложность разработки | ? Низкая | ? Высокая (оркестрация, синхронизация) |
| Отладка | ? Один лог, один процесс | ? Нужно агрегировать логи из разных мест |
| Ресурсы | ? Минимум (общий пул потоков) | ? Избыточно (оверхед на каждый процесс) |
| Надёжность | ?? Падение = остановка всех источников | ? Изоляция сбоев |
| Для 50-100 тиков/сек | ? Идеально | ? Overengineering |

> ?? **Правило:** Начинайте с монолита. Разделяйте только при упоре в лимиты CPU/RAM или при необходимости независимого масштабирования.

## ?? Структура проекта (Clean Architecture)

2. Абстракция клиента (Core/Interfaces/IExchangeClient.cs)

csharp
public interface IExchangeClient : IAsyncDisposable
{
    string Name { get; }
    event Func<Tick, Task> OnTickReceived;
    event Func<string, Task> OnLog;
    Task ConnectAsync(CancellationToken ct);
}

3. Базовый WebSocket с реконнектом (Infrastructure/WebSocket/ResilientWebSocketClient.cs)

csharp
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
 …         await OnMessageReceived(msg, ct);
            }
        }
    }

    protected async Task LogAsync(string m) => Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {m}");
    private async Task CleanupAsync()
    {
        if (_ws?.State == WebSocketState.Open)
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Close", CancellationToken.None);
        _ws?.Dispose();
    }
    public async ValueTask DisposeAsync() { _cts?.Cancel(); await CleanupAsync(); _cts?.Dispose(); }
}

4. Реализация для Kraken (Infrastructure/Exchanges/KrakenClient.cs)

csharp
public class KrakenClient : ResilientWebSocketClient, IExchangeClient
{
    public string Name => "Kraken";
    protected override string Url => "wss://ws.kraken.com";
    public event Func<Tick, Task>? OnTickReceived;
    public event Func<string, Task>? OnLog;

    protected override async Task OnConnected(CancellationToken ct)
    {
        var sub = new { @event = "subscribe", pair = new[] { "XBT/USD", "ETH/USD" }, subscription = new { name = "trade" } };
        await SendAsync(JsonSerializer.Serialize(sub), ct);
        await (OnLog?.Invoke("? Kraken: subscribed") ?? Task.CompletedTask);
    }

    protected override async Task OnMessageReceived(string message, CancellationToken ct)
    {
        if (JsonDocument.TryParse(message, out var doc) && doc.RootElement.ValueKind == JsonValueKind.Array && doc.RootElement.GetArrayLength() >= 4)
        {
            var pair = doc.RootElement[3].GetString();
            foreach (var t in doc.RootElement[1].EnumerateArray())
            {
                var tick = new Tick(
                    Symbol: pair == "XBT/USD" ? "BTC/USD" : pair!,
                    Price: t[0].GetDecimal(),
                    Volume: t[1].GetDecimal(),
                    Timestamp: DateTimeOffset.FromUnixTimeSeconds((long)t[2].GetDouble()),
                    Source: ExchangeType.Kraken,
                    RawId: $"{pair}_{t[2]}"
                );
                if (OnTickReceived != null) await OnTickReceived(tick);
            }
        }
    }

    private async Task SendAsync(string json, CancellationToken ct) =>
        await _ws!.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(json)), WebSocketMessageType.Text, true, ct);
}

5. Pipeline обработки (Application/Pipelines/TickProcessingPipeline.cs)

csharp
public class TickProcessingPipeline : BackgroundService
{
    private readonly Channel<Tick> _channel;
    private readonly IDataRepository _repo;
    private readonly IMonitoringService _monitor;
    private readonly HashSet<string> _recentIds = new();
    private readonly TimeSpan _dedupWindow = TimeSpan.FromSeconds(2);

    public TickProcessingPipeline(IDataRepository repo, IMonitoringService monitor)
    {
        _channel = Channel.CreateBounded<Tick>(new BoundedChannelOptions(10000) { SingleWriter = false, SingleReader = true });
        _repo = repo; _monitor = monitor;
    }

    public async Task SendAsync(Tick tick, CancellationToken ct) => await _channel.Writer.WriteAsync(tick, ct);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await foreach (var tick in _channel.Reader.ReadAllAsync(ct))
        {
            if (_recentIds.Contains(tick.RawId)) continue;
            _recentIds.Add(tick.RawId);
            _ = Task.Delay(_dedupWindow, ct).ContinueWith(_ => _recentIds.Remove(tick.RawId), ct);
            await _repo.SaveAsync(tick, ct);
            _monitor.Increment();
        }
    }
}

6. Composition Root (Host/Program.cs)

csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton<TickProcessingPipeline>();
builder.Services.AddSingleton<IDataRepository, PostgresRepository>();
builder.Services.AddSingleton<IMonitoringService, ConsoleMonitoringService>();
builder.Services.AddSingleton<IExchangeClient, KrakenClient>();
builder.Services.AddHostedService<DataCollectorWorker>();
await builder.Build().RunAsync();

7. Worker-оркестратор (Host/DataCollectorWorker.cs)

csharp
public class DataCollectorWorker : BackgroundService
{
    private readonly IEnumerable<IExchangeClient> _clients;
    private readonly TickProcessingPipeline _pipeline;
    private readonly ILogger<DataCollectorWorker> _logger;

    public DataCollectorWorker(IEnumerable<IExchangeClient> clients, TickProcessingPipeline pipeline, ILogger<DataCollectorWorker> logger)
    {
        _clients = clients; _pipeline = pipeline; _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        foreach (var c in _clients)
        {
            c.OnTickReceived += _pipeline.SendAsync;
            c.OnLog += m => { _logger.LogInformation("{Client}: {Msg}", c.Name, m); return Task.CompletedTask; };
            _ = c.ConnectAsync(ct);
            _logger.LogInformation("?? Started: {Client}", c.Name);
        }
        await Task.Delay(Timeout.Infinite, ct);
    }

    public override async Task StopAsync(CancellationToken ct)
    {
        foreach (var c in _clients) await c.DisposeAsync();
        await base.StopAsync(ct);
    }
}

?? Когда переходить на микросервисы?
Признак
	
Решение
Нагрузка > 1000 тиков/сек на источник
	
Выделить источник в отдельный процесс
Нужна независимая масштабируемость
	
Kubernetes: 1 pod на биржу
Разные SLA/приоритеты
	
Отдельные процессы с разным priority
Командная разработка
	
Разные репозитории/команды
Гео-распределение
	
Сбор ближе к бирже, агрегация в ЦОД
Архитектура будущего:

[Collector 1] -¬
[Collector 2] -+--? RabbitMQ/Kafka (topic: ticks.raw) --? [Aggregator/DB Writer]
[Collector 3] --

?? Рекомендуемый стек
Компонент
	
Выбор
	
Почему
Фреймворк
	
.NET 8 Worker Service
	
Встроенный DI, graceful shutdown
WebSocket
	
ClientWebSocket + Polly
	
Нативный, надёжный
Очередь в памяти
	
System.Threading.Channels
	
Высокая скорость, zero-dependency
БД
	
PostgreSQL + TimescaleDB
	
Оптимизация под временные ряды
Доступ к БД
	
Dapper (write) + EF Core (config)
	
Скорость + удобство миграций
Логирование
	
Serilog
	
Структурированные логи, легко подключить Loki
Мониторинг
	
Консольный счётчик > Prometheus
	
Начинаем просто, масштабируем позже
?? Быстрый старт (CLI)

bash
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

    ? Итог: Начните с одного приложения. Интерфейсы уже абстрагированы, поэтому при росте нагрузки вы легко вынесете сборщики в отдельные процессы, заменив Channel на брокер сообщений.

?? Документ сгенерирован для копирования и сохранения как .md

1
2
3
4
5
6
7
8
9

Если нужно, могу адаптировать структуру под конкретную БД (SQLite/ClickHouse) или добавить готовый docker-compose.yml для TimescaleDB.
