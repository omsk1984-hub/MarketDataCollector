# План рефакторинга MarketDataProcessor

## Текущие проблемы

1. **Polling вместо event-driven**: `ConcurrentQueue` + `Task.Delay(10)` — постоянный опрос очереди
2. **Нет batch-обработки**: каждый тик обрабатывается отдельно — N запросов к БД вместо 1
3. **Нет backpressure**: очередь неограничена, при высокой нагрузке может переполнить память
4. **Дубликат-проверка на каждый тик**: `ExistsAsync` для каждого тика — дорогой запрос

## Архитектура после рефакторинга

```
┌─────────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│  WebSocket       │     │  Channel<T>          │     │  Batch Processor │
│  Clients         │────▶│  (bounded,           │────▶│  (bulk insert)   │
│  (producers)     │     │   backpressure)      │     │                  │
└─────────────────┘     └─────────────────────┘     └──────────────────┘
                                                              │
                                                              ▼
                                                    ┌──────────────────┐
                                                    │  PostgreSQL       │
                                                    │  (bulk insert)   │
                                                    └──────────────────┘
```

## Шаги реализации

### 1. Заменить `ConcurrentQueue` на `Channel<T>`

**Файл**: `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`

```csharp
// Было:
private readonly ConcurrentQueue<(string, decimal, decimal, DateTime, string)> _processingQueue;

// Стало:
private readonly Channel<TickData> _channel;

// Конфигурация:
_channel = Channel.CreateBounded<TickData>(new BoundedChannelOptions(capacity: 10000)
{
    FullMode = BoundedChannelFullMode.Wait,  // backpressure
    SingleReader = true,
    SingleWriter = false
});
```

**Преимущества**:
- Нет polling — consumer ждёт данные асинхронно
- Backpressure при переполнении
- Асинхронный `WriteAsync` не блокирует producer

### 2. Добавить DTO для тика

**Файл**: `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`

```csharp
private readonly record struct TickData(
    string Ticker,
    decimal Price,
    decimal Volume,
    DateTime Timestamp,
    string Exchange
);
```

### 3. Реализовать batch-обработку

**Файл**: `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`

```csharp
private async Task ProcessBatchesAsync(CancellationToken cancellationToken)
{
    var batch = new List<TickData>(_batchSize);
    using var timer = new Timer(_ => FlushBatch(), null, _batchTimeout, _batchTimeout);

    await foreach (var tick in _channel.Reader.ReadAllAsync(cancellationToken))
    {
        batch.Add(tick);
        
        if (batch.Count >= _batchSize)
        {
            await FlushBatchAsync(batch, cancellationToken);
            batch.Clear();
        }
    }
    
    // Финальный flush
    if (batch.Count > 0)
        await FlushBatchAsync(batch, cancellationToken);
}

private async Task FlushBatchAsync(List<TickData> batch, CancellationToken ct)
{
    // 1. Фильтрация дубликатов в памяти (HashSet)
    // 2. Проверка существующих в БД одним запросом
    // 3. Bulk insert через AddRangeAsync
}
```

**Конфигурация**:
- `BatchSize = 100` — размер пачки
- `BatchTimeout = 1000ms` — максимальное время ожидания

### 4. Оптимизировать проверку дубликатов

**Было**: `ExistsAsync` для каждого тика — N запросов к БД

**Стало**:
1. In-memory `HashSet` для дубликатов в текущей пачке
2. Один запрос к БД для проверки существующих: `WHERE (ticker, exchange, timestamp) IN (...)`

**Файл**: `src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs`

```csharp
public async Task<IEnumerable<(string, string, DateTime)>> GetExistingKeysAsync(
    IEnumerable<(string Ticker, string Exchange, DateTime Timestamp)> keys)
{
    // Один запрос для проверки всех ключей пачки
    var keySet = keys.ToList();
    return await _dbSet
        .Where(t => keySet.Any(k => 
            t.Ticker == k.Ticker && 
            t.Exchange == k.Exchange && 
            t.Timestamp == k.Timestamp))
        .Select(t => new { t.Ticker, t.Exchange, t.Timestamp })
        .ToListAsync();
}
```

### 5. Обновить интерфейс

**Файл**: `src/MarketDataCollector.Core/Interfaces/IMarketDataProcessor.cs`

```csharp
public interface IMarketDataProcessor
{
    Task ProcessTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
    Task<int> GetProcessedCountAsync();
    void StartProcessing();
    Task StopProcessingAsync();  // Изменено: async для graceful shutdown
}
```

### 6. Обновить Worker

**Файл**: `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs`

```csharp
finally
{
    await marketDataProcessor.StopProcessingAsync();  // Graceful shutdown
    // ... отключение клиентов
}
```

## Конфигурация

Добавить в `appsettings.json`:

```json
{
  "MarketDataProcessor": {
    "BatchSize": 100,
    "BatchTimeoutMs": 1000,
    "ChannelCapacity": 10000
  }
}
```

## Миграция

1. Создать миграцию для индекса на `(Ticker, Exchange, Timestamp)` — ускорит проверку дубликатов
2. Добавить composite unique constraint

## Ожидаемые улучшения

| Метрика | До | После |
|---------|-----|-------|
| Задержка обработки | до 10ms (polling) | < 1ms (event-driven) |
| Запросы к БД на 100 тиков | 100+ (insert + exists) | 1-2 (bulk insert + check) |
| Потребление памяти | Неограничено | Ограничено backpressure |
| CPU при простое | Polling | 0% (async wait) |

## Файлы для изменения

1. `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs` — основная логика
2. `src/MarketDataCollector.Core/Interfaces/IMarketDataProcessor.cs` — интерфейс
3. `src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs` — bulk операции
4. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs` — вызов StopProcessingAsync
5. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json` — конфигурация
