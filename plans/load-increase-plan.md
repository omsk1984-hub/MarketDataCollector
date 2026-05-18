# План повышения производительности MarketDataCollector

> **Задача:** Обеспечить стабильную работу системы с 15 инструментами (вместо текущих 5)
> **Ожидаемая нагрузка:** ~15 000 msg/s входящих тиков от Binance WebSocket

---

## Текущая архитектура (узкие места)

```mermaid
flowchart LR
    WS1[WebSocket Client\nbtcusdt] -->|тики| CH1
    WS2[WebSocket Client\nethusdt] -->|тики| CH1
    WS3[WebSocket Client\nsolusdt] -->|тики| CH1
    WS4[WebSocket Client\nxrpusdt] -->|тики| CH1
    WS5[WebSocket Client\nadausdt] -->|тики| CH1
    WS6[WebSocket Client\n...ещё 10] -->|тики| CH1

    subgraph MarketDataProcessor
        CH1[Channel\ncapacity=10k\nSingleReader=true]
        RD[1 Consumer\nчитает батчи]
        DEDUP[Дедупликация\nN exists-запросов в БД\nна каждый батч]
        DB[(PostgreSQL)]
    end

    CH1 -->|Wait backpressure| RD
    RD --> DEDUP
    DEDUP -->|BulkInsert| DB

    style CH1 fill:#f96
    style DEDUP fill:#f96
    style RD fill:#f96
```

**Критические проблемы:**
1. **🔴 ChannelCapacity = 10k** — заполнится за < 1 секунды при 15k msg/s
2. **🔴 SingleReader = true** — 1 consumer пишет батч в БД, очередь растёт
3. **🔴 N отдельных `ExistsAsync`** — каждый батч делает N SQL-запросов

---

## Целевая архитектура

```mermaid
flowchart LR
    WS1[15 WebSocket Clients\n~15 000 msg/s] -->|тики| CH1
    
    subgraph MarketDataProcessor
        CH1[Channel\ncapacity=100k\nSingleReader=false]
        C1[Consumer 1\nProcessBatchesAsync]
        C2[Consumer 2\nProcessBatchesAsync]
        C3[Consumer N\nProcessBatchesAsync]
        DEDUP[Массовая проверка\n1 SQL запрос WHERE IN\nна весь батч]
        DB[(PostgreSQL)]
    end

    CH1 --> C1 & C2 & C3
    C1 & C2 & C3 --> DEDUP
    DEDUP -->|BulkInsert\nBatchSize=500| DB

    style CH1 fill:#9f9
    style C1 fill:#9f9
    style C2 fill:#9f9
    style C3 fill:#9f9
    style DEDUP fill:#9f9
```

---

## Todo-лист для реализации

### [ ] Шаг 1: Добавить метод `ExistsBatchAsync` в RawTickRepository

**Файлы:**
- [`IRawTickRepository.cs`](src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs) — добавить метод в интерфейс
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) — реализовать массовый запрос `WHERE IN`

**Суть изменения:**
```csharp
// Вместо N отдельных запросов:
foreach (var tick in ticks)
    await _rawTickRepository.ExistsAsync(tick.Ticker, tick.Exchange, tick.Timestamp, ct);

// Один массовый запрос:
await _rawTickRepository.ExistsBatchAsync(keys, ct);
// SQL: SELECT ticker, exchange, timestamp FROM "RawTicks" 
//      WHERE (ticker, exchange, timestamp) IN (@p0,@p1,@p2), ...
```

### [ ] Шаг 2: Переписать `ProcessBatchAsync` в MarketDataProcessor

**Файл:** [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs)

- Заменить цикл с `ExistsAsync` на один вызов `ExistsBatchAsync`
- **Не удалять** остальную логику (группировка дубликатов в памяти — оставить для оптимизации)

### [ ] Шаг 3: Обновить конфигурацию в appsettings.json

**Файл:** [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json)

| Параметр | Было | Стало |
|----------|------|-------|
| `MarketDataProcessor.BatchSize` | 100 | 500 |
| `MarketDataProcessor.ChannelCapacity` | 10000 | 100000 |
| `TickAggregator.ChannelCapacity` | 10000 | 100000 |
| `WebSocketClient.ReceiveBufferSize` | 4096 | 16384 |

### [ ] Шаг 4: Параллельные consumer'ы для Channel

**Файл:** [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs)

- Убрать `SingleReader = true` → `SingleReader = false`
- В `StartProcessingAsync` запускать `Environment.ProcessorCount` параллельных `ProcessBatchesAsync`
- `_processingTask` становится `Task` (не `Task.WhenAll`), т.к. `Task.WhenAll` возвращает `Task`
- В `StopProcessingAsync` дожидаться `_processingTask` как обычно

```csharp
public Task StartProcessingAsync(CancellationToken cancellationToken = default)
{
    var consumerCount = Environment.ProcessorCount; // 4-8 на типичной системе
    var consumers = Enumerable.Range(0, consumerCount)
        .Select(_ => ProcessBatchesAsync(cancellationToken));
    _processingTask = Task.WhenAll(consumers);
    return Task.CompletedTask;
}
```

### [ ] Шаг 5: `INSERT ... ON CONFLICT DO NOTHING` вместо дедупликации

**Файлы:**
- [`IRawTickRepository.cs`](src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs) — добавить метод
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) — реализовать bulk insert с `ON CONFLICT DO NOTHING`
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — упростить `ProcessBatchAsync`

**SQL для вставки:**
```sql
INSERT INTO "RawTicks" ("Id", "Ticker", "Price", "Volume", "Timestamp", "Exchange", "Normalized", "CreatedAt", ...)
VALUES ...
ON CONFLICT ("Ticker", "Exchange", "Timestamp") DO NOTHING;
```

**После этого шага** можно полностью удалить `GetExistingKeysFromDbAsync` из MarketDataProcessor.

---

## Порядок выполнения

```mermaid
flowchart TD
    A[Шаг 1: ExistsBatchAsync\nв репозиторий] --> B[Шаг 2: Использовать\nв MarketDataProcessor]
    B --> C[Шаг 3: Конфигурация\nappsettings.json]
    C --> D[Шаг 4: Параллельные\nconsumer'ы]
    D --> E[Шаг 5: ON CONFLICT\nDO NOTHING]
    
    style A fill:#bbf
    style B fill:#bbf
    style C fill:#bfb
    style D fill:#fbb
    style E fill:#fbb
```

- **Синие шаги (1-2)** — критически важны, без них система не выдержит нагрузку
- **Зелёный шаг (3)** — тривиальное изменение конфига
- **Красные шаги (4-5)** — опциональны, дают дополнительный прирост

---

## Ожидаемые метрики после реализации

| Метрика | Сейчас (5 инстр.) | После шагов 1-3 | После шагов 1-5 |
|---------|:-:|:-:|:-:|
| Инструментов | 5 | 15 | 15 |
| Входящих msg/s | ~5 000 | ~15 000 | ~15 000 |
| Обработано ticks/s | ~3 000 | ~12 000+ | ~15 000 |
| SQL-запросов EXISTS на батч | 100 | 1 | 0 |
| BatchSize | 100 | 500 | 500-1000 |
| Channel capacity | 10 000 | 100 000 | 100 000 |
| Consumer'ов | 1 | 1 | N (CPU count) |
| Запись в БД | AddRangeAsync | AddRangeAsync | BulkInsert ON CONFLICT |
