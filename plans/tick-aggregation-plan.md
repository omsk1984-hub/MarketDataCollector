# План разработки агрегации тиков (OHLCV) — 1m свечи

## 1. Текущее состояние

В проекте уже есть:
- **Сущность [`AggregatedData`](src/MarketDataCollector.Domain/Entities/AggregatedData.cs)** — OHLCV-свеча с полями: `Ticker`, `Interval`, `OpenPrice`, `HighPrice`, `LowPrice`, `ClosePrice`, `Volume`, `StartTime`, `EndTime`
- **Таблица `AggregatedData`** в [`init.sql`](docker/init.sql:33) — уже создаётся при инициализации БД
- **DbSet `AggregatedData`** в [`MarketDataDbContext`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs:10)
- **Индекс** на `(Ticker, Interval)` и `StartTime` в [`OnModelCreating`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs:43)

**Чего НЕ хватает:**
- Репозиторий для `AggregatedData` (интерфейс + реализация)
- Сервис-агрегатор, который преобразует поток `RawTick` → 1m OHLCV-свечи
- Конфигурация агрегации
- Интеграция агрегатора в существующий пайплайн
- Регистрация новых сервисов в DI

---

## 2. Архитектура (с учётом ваших решений)

```
WebSocket → BinanceWebSocketClient
  → MarketDataProcessor.ProcessTickAsync()
    → Channel<TickData> (основной, для RawTicks)
      → ProcessBatchesAsync() → ProcessBatchAsync() → RawTickRepository
    → Channel<TickData> (отдельный, для агрегации)  ← НОВЫЙ
      → TickAggregator (in-memory OHLCV, 1m)
        → По таймеру: flush завершённых свечей в AggregatedDataRepository
```

### Ключевые решения (согласованы с вами):

1. **Отдельный Channel** — агрегатор получает тики через свой собственный Channel, независимо от батчевой записи RawTicks. Это даёт:
   - Независимость: агрегатор не ждёт записи RawTicks
   - Меньшая задержка для свечных данных
   - Возможность отключить агрегацию без влияния на основной пайплайн

2. **Только 1m интервал** — минимальная свеча, достаточная для большинства сценариев

3. **Чистый старт** — при запуске агрегатор не загружает незавершённые свечи из БД, начинает с нуля

4. **Простой Insert** — дубликаты исключены уникальным ключом `(Ticker, Exchange, Interval, StartTime)`

---

## 3. Детальный план реализации

### Шаг 1: Интерфейс `IAggregatedDataRepository` (Core)

**Файл:** `src/MarketDataCollector.Core/Interfaces/IAggregatedDataRepository.cs`

```csharp
public interface IAggregatedDataRepository : IRepository<AggregatedData>
{
    Task<IEnumerable<AggregatedData>> GetByTickerAndIntervalAsync(
        string ticker, string interval,
        DateTime? from = null, DateTime? to = null,
        CancellationToken cancellationToken = default);
}
```

### Шаг 2: Репозиторий `AggregatedDataRepository` (Infrastructure)

**Файл:** `src/MarketDataCollector.Infrastructure/Repositories/AggregatedDataRepository.cs`

- Реализовать `IAggregatedDataRepository`
- Использовать `MarketDataDbContext` и `DbSet<AggregatedData>`
- Метод `GetByTickerAndIntervalAsync` — фильтр по `Ticker`, `Interval`, опционально `StartTime >= from` и `StartTime <= to`

### Шаг 3: Конфигурация `TickAggregatorOptions` (Core)

**Файл:** `src/MarketDataCollector.Core/Configuration/TickAggregatorOptions.cs`

```csharp
public class TickAggregatorOptions
{
    public const string SectionName = "TickAggregator";
    
    public int FlushIntervalSeconds { get; set; } = 5;
    public int ChannelCapacity { get; set; } = 10000;
}
```

### Шаг 4: Интерфейс `ITickAggregator` (Core)

**Файл:** `src/MarketDataCollector.Core/Interfaces/ITickAggregator.cs`

```csharp
public interface ITickAggregator
{
    Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
```

### Шаг 5: Реализация `TickAggregator` (Application)

**Файл:** `src/MarketDataCollector.Application/Services/TickAggregator.cs`

Ключевая логика:

```csharp
public class TickAggregator : ITickAggregator
{
    private readonly Channel<TickData> _channel;
    private readonly ConcurrentDictionary<string, InMemoryCandle> _activeCandles = new();
    private readonly TimeSpan _interval = TimeSpan.FromMinutes(1);
    private Timer _flushTimer;
    
    // TickData — внутренняя record struct (аналогично MarketDataProcessor.TickData)
    
    public Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
    {
        return _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange)).AsTask();
    }
    
    // Фоновая задача: читает из Channel и обновляет in-memory свечи
    private async Task ProcessChannelAsync(CancellationToken ct)
    {
        await foreach (var tick in _channel.Reader.ReadAllAsync(ct))
        {
            var bucketStart = RoundDown(tick.Timestamp, _interval);
            var key = $"{tick.Ticker}|{tick.Exchange}|{bucketStart:O}";
            
            var candle = _activeCandles.GetOrAdd(key, _ => new InMemoryCandle
            {
                Ticker = tick.Ticker,
                Exchange = tick.Exchange,
                Interval = "1m",
                StartTime = bucketStart,
                EndTime = bucketStart + _interval,
                Open = tick.Price,
                High = tick.Price,
                Low = tick.Price,
                Close = tick.Price,
                Volume = tick.Volume
            });
            
            candle.Update(tick.Price, tick.Volume);
        }
    }
    
    // Таймер: каждые N секунд сбрасывает завершённые свечи в БД
    private async Task FlushCompletedCandlesAsync()
    {
        var now = _timeService.UtcNow;
        var completed = _activeCandles
            .Where(kvp => kvp.Value.EndTime <= now)
            .ToList();
        
        foreach (var kvp in completed)
        {
            _activeCandles.TryRemove(kvp.Key, out _);
        }
        
        if (completed.Count > 0)
        {
            var entities = completed.Select(kvp => kvp.Value.ToAggregatedData(_timeService)).ToList();
            await _repository.AddRangeAsync(entities);
            await _repository.SaveChangesAsync();
        }
    }
}
```

### Шаг 6: Интеграция с `MarketDataProcessor`

В [`MarketDataProcessor.ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:57) после записи в основной Channel добавить запись в Channel агрегатора:

```csharp
public async Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
{
    await _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange));
    
    // Передаём тик в агрегатор (если он подключён)
    if (_tickAggregator != null)
    {
        await _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
    }
    
    _logger.LogDebug("Тик добавлен в очередь: {Ticker} {Price} {Volume} {Exchange}", ticker, price, volume, exchange);
}
```

**Вариант подключения:** через DI — `ITickAggregator` опционально (может быть `null`, если агрегация не настроена).

### Шаг 7: Регистрация в DI

В [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs):

```csharp
// Configuration
builder.Services.Configure<TickAggregatorOptions>(
    builder.Configuration.GetSection(TickAggregatorOptions.SectionName));

// Aggregation
builder.Services.AddSingleton<ITickAggregator, TickAggregator>();
builder.Services.AddScoped<IAggregatedDataRepository, AggregatedDataRepository>();
```

### Шаг 8: Конфигурация в `appsettings.json`

```json
"TickAggregator": {
    "FlushIntervalSeconds": 5,
    "ChannelCapacity": 10000
}
```

### Шаг 9: Запуск/остановка агрегатора в `Worker`

В [`Worker.RunWithRecoveryAsync`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs):

```csharp
// Запуск агрегатора
var aggregator = scope.ServiceProvider.GetRequiredService<ITickAggregator>();
await aggregator.StartAsync(stoppingToken);

// В CleanupAsync:
await aggregator.StopAsync(stoppingToken);
```

---

## 4. Диаграмма потока данных

```mermaid
flowchart LR
    WS[WebSocket\nBinance] -->|Tick| MP[MarketDataProcessor]
    
    MP -->|Channel 1\nbatch write| RR[RawTickRepository\nPostgreSQL]
    MP -->|Channel 2\nindependent| TA[TickAggregator\nIn-Memory OHLCV 1m]
    
    TA -->|Timer flush every 5s| AR[AggregatedDataRepository\nPostgreSQL]
    
    subgraph In-Memory State
        TA_Candles[Active Candles\nConcurrentDictionary\nKey: Ticker|Exchange|BucketStart]
    end
    
    TA --> TA_Candles
    TA_Candles --> TA
```

---

## 5. Стратегия обработки

1. **При старте** — агрегатор начинает с пустого словаря свечей
2. **Каждый тик** — округляется до 1m бакета, обновляет соответствующую свечу в `ConcurrentDictionary`
3. **Каждые 5 секунд** — таймер собирает все завершённые свечи (EndTime <= Now), сохраняет в БД и удаляет из памяти
4. **При остановке** — форсированный flush всех активных свечей (включая незавершённые)
5. **Дубликаты** — исключены уникальным ключом `(Ticker, Exchange, Interval, StartTime)` в БД

---

## 6. Тестирование

### Unit-тесты для `TickAggregator`:
- Проверка корректности OHLCV для одного тика
- Проверка корректности OHLCV для нескольких тиков в одной 1m свече
- Проверка перехода на новую минуту (сброс свечи)
- Проверка `RoundDown` для 1m интервала
- Проверка flush завершённых свечей

### Интеграционные тесты:
- Проверка сохранения завершённых свечей в БД через репозиторий

---

## 7. Оценка сложности

| Шаг | Описание | Сложность |
|-----|----------|-----------|
| 1 | `IAggregatedDataRepository` интерфейс | Низкая |
| 2 | `AggregatedDataRepository` реализация | Низкая |
| 3 | `TickAggregatorOptions` конфигурация | Низкая |
| 4 | `ITickAggregator` интерфейс | Низкая |
| 5 | `TickAggregator` реализация | Средняя |
| 6 | Интеграция с `MarketDataProcessor` | Низкая |
| 7 | Регистрация в DI | Низкая |
| 8 | Конфигурация `appsettings.json` | Низкая |
| 9 | Запуск/остановка в `Worker` | Низкая |
| 10 | Unit-тесты | Средняя |