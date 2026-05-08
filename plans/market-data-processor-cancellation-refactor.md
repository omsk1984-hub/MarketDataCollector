# План рефакторинга: CancellationToken в MarketDataProcessor

## Цель
Добавить сквозную поддержку `CancellationToken` от `Worker.stoppingToken` до всех асинхронных операций в `MarketDataProcessor`.

## Текущие проблемы
1. `StartProcessing()` не принимает `CancellationToken` — нельзя отменить обработку
2. `ProcessBatchesAsync()` не получает токен — `ReadAllAsync` не реагирует на отмену
3. Неиспользуемый `Timer` и пустой `FlushBatch()` — мёртвый код
4. `ProcessBatchAsync()` и `GetExistingKeysFromDbAsync()` не поддерживают отмену

## Шаги реализации

### 1. Обновить интерфейс IMarketDataProcessor
```csharp
// Было:
void StartProcessing();

// Стало:
void StartProcessing(CancellationToken cancellationToken);
```

### 2. Обновить MarketDataProcessor.StartProcessing
```csharp
public void StartProcessing(CancellationToken cancellationToken = default)
{
    if (_processingTask != null && !_processingTask.IsCompleted)
        return;

    _processingTask = Task.Run(async () => await ProcessBatchesAsync(cancellationToken));
    _logger.LogInformation("Market data processor started");
}
```

### 3. Обновить ProcessBatchesAsync
```csharp
private async Task ProcessBatchesAsync(CancellationToken cancellationToken)
{
    var batch = new List<TickData>(_batchSize);

    try
    {
        await foreach (var tick in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            batch.Add(tick);

            if (batch.Count >= _batchSize)
            {
                await ProcessBatchAsync(batch, cancellationToken);
                batch.Clear();
            }
        }
    }
    catch (OperationCanceledException)
    {
        _logger.LogInformation("Processing was cancelled");
    }
    catch (ChannelClosedException)
    {
        // Expected when channel is completed
    }
    finally
    {
        // Финальный flush
        if (batch.Count > 0)
        {
            await ProcessBatchAsync(batch, cancellationToken);
        }
    }
}
```

### 4. Обновить ProcessBatchAsync
```csharp
private async Task ProcessBatchAsync(List<TickData> batch, CancellationToken cancellationToken)
{
    try
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        // ... существующая логика ...
        
        await _rawTickRepository.AddRangeAsync(entities, cancellationToken);
        await _rawTickRepository.SaveChangesAsync(cancellationToken);
        
        // ... остальная логика ...
    }
    catch (OperationCanceledException)
    {
        _logger.LogWarning("Batch processing was cancelled");
        throw;
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Error processing batch of {Count} ticks", batch.Count);
    }
}
```

### 5. Обновить GetExistingKeysFromDbAsync
```csharp
private async Task<HashSet<(string, string, DateTime)>> GetExistingKeysFromDbAsync(
    List<TickData> ticks, 
    CancellationToken cancellationToken)
{
    var existing = new HashSet<(string, string, DateTime)>();
    
    foreach (var tick in ticks)
    {
        cancellationToken.ThrowIfCancellationRequested();
        
        if (await _rawTickRepository.ExistsAsync(tick.Ticker, tick.Exchange, tick.Timestamp))
        {
            existing.Add((tick.Ticker, tick.Exchange, tick.Timestamp));
        }
    }
    
    return existing;
}
```

### 6. Удалить мёртвый код
- Удалить `private readonly TimeSpan _batchTimeout;`
- Удалить `using var timer = ...` и `FlushBatch()`
- Удалить `private readonly object _lockObject;` (не используется)

### 7. Обновить Worker.cs
```csharp
// Было:
marketDataProcessor.StartProcessing();

// Стало:
marketDataProcessor.StartProcessing(stoppingToken);
```

## Диаграмма потока данных

```
Worker.ExecuteAsync(stoppingToken)
    │
    ├── client.StartAsync(stoppingToken)
    │
    ├── marketDataProcessor.StartProcessing(stoppingToken)
    │       │
    │       └── Task.Run → ProcessBatchesAsync(ct)
    │               │
    │               ├── channel.Reader.ReadAllAsync(ct)
    │               │
    │               └── ProcessBatchAsync(batch, ct)
    │                       │
    │                       ├── GetExistingKeysFromDbAsync(ticks, ct)
    │                       │
    │                       └── repository.AddRangeAsync(entities, ct)
    │
    └── Cleanup → StopProcessingAsync(ct)
            │
            └── channel.Writer.Complete()
```

## Файлы для изменения
1. `src/MarketDataCollector.Core/Interfaces/IMarketDataProcessor.cs`
2. `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`
3. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs`
