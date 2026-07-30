# План выполнения улучшений производительности

На основе: [`plans/improvements-analysis-20260730.md`](plans/improvements-analysis-20260730.md)

Приоритеты: **P0** (критично) → **P3** (low)

---

## P0: Замена BulkCopyAsync на UNNEST (главный bottleneck)

**Файлы:**
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) — переписать `BulkCopyAsync`
- [`IRawTickRepository.cs`](src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs) — если меняется сигнатура

**Суть:**
- Убрать DROP TABLE + CREATE TEMP TABLE + Binary COPY + INSERT FROM temp
- Вместо этого: один `INSERT INTO ... SELECT unnest(@ids), unnest(@tickers), ... ON CONFLICT DO NOTHING`
- `NpgsqlParameter` с массивными значениями (`NpgsqlDbType.Array | NpgsqlDbType.Uuid` etc.)
- Упростить retry-логику: убрать экспоненциальную задержку, оставить 1 повтор с плоской задержкой

**Ожидаемый эффект:** +50-100% пропускной способности (10K → 15-20K ticks/sec)

---

## P0: Увеличение DeduplicationCacheMaxSize до 50000

**Файлы:**
- [`MarketDataProcessorOptions.cs`](src/MarketDataCollector.Core/Configuration/MarketDataProcessorOptions.cs) — `DeduplicationCacheMaxSize = 6000` → `50000`
- [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) — та же правка

**Ожидаемый эффект:** Кэш покрывает ~5 секунд вместо ~0.6 сек, выше hit-rate дедупликации

---

## P1: Увеличение BatchSize до 4000 и ChannelCapacity до 50000

**Файлы:**
- [`MarketDataProcessorOptions.cs`](src/MarketDataCollector.Core/Configuration/MarketDataProcessorOptions.cs) — `BatchSize = 800` → `4000`, `ChannelCapacity = 10000` → `50000`
- [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) — `BatchSize: 1000` → `4000`, `ChannelCapacity: 150000` → `50000`

**Ожидаемый эффект:** +20-30% (меньше транзакций на 1K тиков)

---

## P1: Убрать Interlocked в Single Consumer mode

**Файлы:**
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — `ProcessBatchAsync` строка 581-582

**Суть:**
- `ProcessBatchAsync` вызывается только из consumer-потока. В SingleReader mode гарантировано 1 consumer, 1 поток
- Заменить `Interlocked.Add(ref _totalReceivedCount, batchSize)` на `_totalReceivedCount += batchSize`
- Заменить `Interlocked.Add(ref _processedCount, inserted)` на `_processedCount += inserted`
- В `ProcessTickAsync` (line 114, 145) Interlocked оставить — этот метод может вызываться из разных потоков

---

## P2: Вынести TickData в общую модель Domain

**Файлы:**
- Создать [`TickData.cs`](src/MarketDataCollector.Domain/Entities/TickData.cs) — `readonly record struct` в Domain
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — удалить дублированное объявление, использовать общее
- [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs) — удалить дублированное объявление, использовать общее

---

## P2: Оптимизация DeduplicationCache с DedupKey

**Файлы:**
- [`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) — заменить `Dictionary<(string, string, DateTime), byte>` на `Dictionary<DedupKey, byte>`

**Суть:**
- `DedupKey` как `readonly record struct` с `IEquatable<DedupKey>` и кастомным `GetHashCode`
- `DateTime` → `long TimestampTicks` для избежания boxing/hashing DateTime
- `HashCode.Combine()` быстрее ValueTuple

---

## P2: Batch-эвикция в DeduplicationCache

**Файлы:**
- [`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) — заменить while-цикл на эвикцию раз в N операций

**Суть:**
- Вместо `while (_cache.Count >= _maxSize)` — проверять раз в K добавлений
- Эвиктировать сразу batch старых записей (например, 10% от maxSize)

---

## P3: Добавить метрику времени записи батча

**Файлы:**
- [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs) — добавить `Histogram<double> BatchWriteDuration`
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — записывать время вокруг `BulkCopyAsync`

---

## P3: Починить channel_index в метриках TicksReceived

**Файлы:**
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — line 585-587, передавать реальный `channelIndex`

**Суть:**
- Сейчас хардкод `channel_index=0` даже в multi-consumer mode
- Исправить на `new KeyValuePair<string, object?>("channel_index", channelIndex)`

---

## P3: Периодическая запись channel fill level

**Файлы:**
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — в `ProcessBatchesAsync` добавить периодическую запись fill level

**Суть:**
- Каждые N секунд (например, 10) записывать `MarketDataTelemetry.ChannelFill.Record(count, ...)`
- Или записывать при каждом сбросе батча

---

## P3: Memory pooling для батчей

**Файлы:**
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) — использовать `ArrayPool<TickData>` для `filteredTicks` и `entities`

**Суть:**
- Вместо `new List<TickData>(batch.Count)` — арендовать массив из `ArrayPool<TickData>.Shared`
- Переиспользовать `NpgsqlParameter[]` если останется UNNEST (зависит от реализации P0)

---

## Архитектурные улучшения (опционально)

### Упростить retry-логику BulkCopyAsync

**Файлы:**
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) — упростить retry

**Суть:**
- 5 попыток с экспоненциальной задержкой — избыточно. Deadlock'и невозможны (per-ticker routing)
- Оставить 1 retry с плоской задержкой ~200ms как safety-net

### BulkInsertIgnoreConflictsAsync — удалить или отметить как deprecated

**Файлы:**
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) — метод `BulkInsertIgnoreConflictsAsync`
- [`IRawTickRepository.cs`](src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs)

**Суть:**
- Метод не используется в production-коде (используется `BulkCopyAsync`)
- Удалить или пометить `[Obsolete]`

---

## ⚠️ Зависимости

1. P0 (UNNEST) — самое risky изменение, требует тщательного тестирования
2. P2 (DedupKey struct) — меняет внутренний API DeduplicationCache, нужно обновить тесты
3. Все P3 — независимы, можно делать в любом порядке

## 🧪 Тестирование

После каждого изменения:
1. `dotnet build` — успешная компиляция
2. Unit-тесты: `dotnet test tests/MarketDataCollector.Tests`
3. Интеграционные тесты с PostgreSQL
4. Benchmark-прогон для проверки прироста производительности
