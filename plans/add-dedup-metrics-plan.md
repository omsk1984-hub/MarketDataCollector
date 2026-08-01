# План: метрики дедупликации DeduplicationCache vs ON CONFLICT

## Цель

Сейчас невозможно различить, сколько тиков отсеивается на каждом из двух уровней дедупликации
внутри одного батча. Есть только:
- `ticks.processed` — реально вставлено в БД (`inserted`).
- `cachedCount` — сколько отсеял `DeduplicationCache` — но только как `Activity.SetTag("cached.count")`
  и в `LogPeriodicProgress`, **не как Prometheus-метрика**.

Это мешает интерпретировать разницу `incoming - processed` (в прогоне 093357 = 51,224):
непонятно, сколько из них отсеяно кэшем в процессе, а сколько — `ON CONFLICT` в БД.

## Проверка текущего состояния (перепроверено)

Метрики в [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs):
- `ticks.incoming`, `ticks.received`, `ticks.processed`, `ticks.dropped`, `ticks.dropped.silently` — есть.
- **Отдельных метрик для `DeduplicationCache`-отсева и `ON CONFLICT`-отсева — НЕТ.**

Ключевые данные доступны в [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:824):
- `batchSize` — прочитано из канала в батч.
- `cachedCount` — отсев `DeduplicationCache` (дубликаты внутри прогона).
- `writeIdx = batchSize - cachedCount` — тики, ушедшие в SQL-запрос.
- `inserted` — вставлено в БД (возврат `BulkInsertFastAsync`, точное число для `ON CONFLICT DO NOTHING`).
- **`onConflictSkipped = writeIdx - inserted`** — отсев на уровне БД.

Проверено: `BulkInsertFastAsync` возвращает `ExecuteSqlRawAsync` — для `ON CONFLICT DO NOTHING` это число
фактически вставленных строк (не затронутых). См. [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:385).

Формулы для одного батча:
```
batchSize        = cachedCount + writeIdx
writeIdx         = inserted + onConflictSkipped
onConflictSkipped = writeIdx - inserted
```

Т.е. `incoming - processed = Σ(cachedCount) + Σ(onConflictSkipped)` — два источника, которые нужно разделить.

## Изменения

### 1. Новые счётчики в `MarketDataTelemetry.cs`

Добавить два `Counter<long>` рядом с `TicksProcessed`:

| Метрика | Имя | Смысл | Теги |
|---|---|---|---|
| `TicksDeduplicatedByCache` | `ticks.deduplicated.cache` | Отсев `DeduplicationCache` | `channel_index` |
| `TicksDeduplicatedByDb` | `ticks.deduplicated.db` | Отсев `ON CONFLICT` в БД | `channel_index` |

Пример объявления (аналогично `TicksProcessed`):
```csharp
public static readonly Counter<long> TicksDeduplicatedByCache = Instance.CreateCounter<long>(
    name: "ticks.deduplicated.cache",
    unit: "count",
    description: "Ticks filtered by in-process DeduplicationCache within a batch");
```

Тег `channel_index` — через существующий `ChannelTag(channelIndex)` (как у `TicksReceived`),
чтобы было видно распределение по каналам и корреляция с дропами.

### 2. Запись метрик в `ProcessBatchAsync` (`MarketDataProcessor.cs`)

В блоке после `inserted` (сейчас строки ~900-911), рядом с `TicksReceived.Add` / `TicksProcessed.Add`,
добавить инкремент обоих новых счётчиков:
```csharp
int deduplicatedByCache = cachedCount;                 // отсев DeduplicationCache
int deduplicatedByDb    = batchSize - cachedCount - inserted; // отсев ON CONFLICT

MarketDataTelemetry.TicksDeduplicatedByCache.Add(deduplicatedByCache, ChannelTag(channelIndex));
MarketDataTelemetry.TicksDeduplicatedByDb.Add(deduplicatedByDb, ChannelTag(channelIndex));
```

### 3. (Опционально) Теги в `BulkInsertFastAsync`

Текущего возвращаемого значения `inserted` достаточно — править `RawTickRepository.cs` не требуется.
Если позже понадобится точное число попавших в конфликт без дополнительной арифметики — можно добавить
`RETURNING`-подсчёт, но это лишний round-trip. На данном этапе арифметика в `ProcessBatchAsync` оптимальна.

## Проверка

- `dotnet build MarketDataCollector.sln` — компиляция без ошибок/предупреждений.
- `dotnet test` затронутых тестов `MarketDataProcessorTests` — убедиться, что новых регрессий нет
  (учитывая предсуществующие падения по лог-сообщениям, см. текущий анализ).
- Ручной прогон load-теста: в `counters_*.csv` должны появиться `ticks.deduplicated.cache` и
  `ticks.deduplicated.db`, при этом должно выполняться
  `ticks.received - ticks.processed == ticks.deduplicated.cache + ticks.deduplicated.db`
  (без учёта редких сбоев записи).
