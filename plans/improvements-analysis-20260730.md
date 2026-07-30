# Предложения по улучшению системы

Основано на анализе прогона: 900,000 тиков, ~87 сек, 10K ticks/sec.

---

## 1. 🚀 Критические улучшения производительности

### 1.1 DROP+CREATE temp table на каждый батч — BOTTLENECK

**Проблема:** [`RawTickRepository.BulkCopyAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:256) при каждом вызове (~900 раз за прогон) выполняет:
```sql
DROP TABLE IF EXISTS rawticks_staging;
CREATE TEMP TABLE rawticks_staging (...);
-- Binary COPY
-- INSERT INTO rawticks ... SELECT ... ON CONFLICT DO NOTHING
```

DDL-операции медленные и генерируют лишние автовакуумы. Это ДОРОЖЕ, чем сам COPY.

**Решение:** 
- Использовать `UNNEST` с массивами параметров (Npgsql поддерживает `NpgsqlParameter` с `Value = new[] { ... }`):
  ```sql
  INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized)
  SELECT unnest(@ids), unnest(@tickers), unnest(@prices), unnest(@volumes), 
         unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)
  ON CONFLICT (ticker, exchange, timestamp) DO NOTHING;
  ```
- **Выигрыш:** ~10-50x быстрее, нет DDL, нет автовакуума

### 1.2 DeduplicationCache — аллокации кортежей

**Проблема:** [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) использует `(string, string, DateTime)` Dictionary. На каждый `Contains()` и `Add()` аллоцируется ValueTuple + хэши трёх полей.

**Решение:** 
- Использовать `CompositeKey` как `readonly record struct` с имплементацией `IEquatable<>` и кастомным `GetHashCode`:
  ```csharp
  internal readonly record struct DedupKey(string Ticker, string Exchange, long TimestampTicks) 
      : IEquatable<DedupKey>;
  ```
- `DateTime` → `long TimestampTicks` — меньше аллокаций при хэшировании
- Дополнительно: комбинированный хэш через `HashCode.Combine()` — быстрее ValueTuple

### 1.3 DeduplicationCache — while-эвикция

**Проблема:** В цикле эвикции (`while (_cache.Count >= _maxSize)`) каждый вызов `Dequeue()` + `Remove()` — O(1), но 3 операции. Для 900K тиков ≈ 900K операций удаления.

**Решение:** 
- В `_order` хранить не ключи, а `(key, DateTime)` для batch-эвикции раз в N операций
- Или использовать `LinkedListNode` для O(1) удаления из середины (но это сложнее)

### 1.4 Memory pooling для батчей

**Проблема:** На каждый батч создаются:
1. `List<TickData> filteredTicks = new(batch.Count)` — аллокация
2. `var entities = filteredTicks.Select(...).ToList()` — ещё одна аллокация
3. Параметры `NpgsqlParameter[]` — аллокация массива

**Решение:** 
- Использовать `ArrayPool<TickData>.Shared` или пул листов
- `NpgsqlParameter[]` — переиспользовать, обновляя значения
- Для 900 батчей × 3 аллокации = 2700+ аллокаций — убираем garabage pressure

---

## 2. 🔧 Архитектурные улучшения

### 2.1 TickData — дублирование структуры

**Проблема:** [`TickData`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:46) объявлена и в `MarketDataProcessor` (line 46) и в [`TickAggregator`](src/MarketDataCollector.Application/Services/TickAggregator.cs:42) — дублирование.

**Решение:** Вынести в общую модель `MarketDataCollector.Domain` как `readonly record struct`.

### 2.2 Конфигурация batch size и channel capacity

**Проблема:** [`BatchSize=800`](src/MarketDataCollector.Core/Configuration/MarketDataProcessorOptions.cs:12) по умолчанию, а в тесте батчи были по 1000 (что указывает на другую конфигурацию). Значение 800 — из бенчмарка от 2025 года, а текущая производительность выше.

**Решение:** 
- Поставить `BatchSize=2000` или `4000` — больше тиков за одну транзакцию, меньше overhead на COPY + INSERT
- Увеличить `ChannelCapacity` с 10,000 до 50,000 — буфер на случай spikes

### 2.3 Удалить deadlock retry (код-мёртв?)

**Проблема:** [`BulkCopyAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:239) содержит retry-loop на 5 попыток с экспоненциальной задержкой и jitter. Но сам же код утверждает: "deadlock'и невозможны" (line 223-225). Если это правда — код мёртв.

**Решение:** 
- Либо убрать retry (упрощение кода)
- Либо оставить как safety-net, но снять экспоненциальную задержку — достаточно 1 retry с плоской задержкой

### 2.4 Interlocked в Single Consumer mode — лишний

**Проблема:** `Interlocked.Add(ref _totalReceivedCount, batchSize)` (line 581) и `Interlocked.Increment(ref _totalIncomingCount)` (line 114). В Single Consumer mode гонки нет, а `Interlocked` дороже обычной записи (барьер памяти).

**Решение:** Использовать обычные присваивания в Single Consumer mode.

---

## 3. 📊 Наблюдаемость (Observability)

### 3.1 Channel fill level — только на старте и стопе

**Проблема:** `processor_channel_fill_count` записывается только при StartProcessingAsync (line 169) и StopProcessingAsync (line 305). В прогоне — 1 запись на старте с sum=0. Это не даёт картины заполненности канала во времени.

**Решение:** 
- Добавить периодическую запись fill level (раз в 5-10 секунд) в фоновом loop
- Или записывать при каждом сбросе батча

### 3.2 Нет метрики по времени записи батча

**Проблема:** Нет гистограммы `ticks_batch_write_duration` — нельзя оценить latency записи.

**Решение:** Добавить гистограмму с хэдерами `channel_index`, `batch_size`, `inserted_count`.

### 3.3 ticks_batch_size_count — channel_index всегда 0

**Проблема:** [`TicksReceived.Add`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:585) хардкодит `channel_index=0`. Даже в Multi Consumer mode нельзя отличить каналы.

**Решение:** Использовать `channelIndex` из аргумента метода.

---

## 4. 🧪 Тестирование

### 4.1 Нет теста на DeduplicationCache при maxSize=6000 с реальными данными

**Проблема:** Кэш размером 6000 — при 10K ticks/sec он полностью обновляется за ~0.6 сек. Это значит, что кэш ловит только intra-batch дубли и дубли в пределах 0.6 сек.

**Решение:** 
- Увеличить кэш до 50,000-100,000 для покрытия 5-10 секунд
- Либо добавить тест с замером hit-rate кэша в прогоне (сейчас его нет)

---

## 5. ⚡ Оценка потенциального прироста

| Улучшение | Ожидаемый эффект |
|---|---|
| Убрать DDL из BulkCopy (UNNEST) | +50-100% (10K → 15-20K ticks/sec) |
| Увеличить BatchSize до 4000 | +20-30% (меньше транзакций) |
| Memory pooling | -5-10% GC pause time |
| DeduplicationCache оптимизация | -3-5% CPU на аллокациях |
| **Суммарно** | **+80-150% к текущей производительности** |

---

## 6. 🎯 Приоритет внедрения

1. **P0:** BulkCopyAsync → UNNEST (снимаем bottleneck)
2. **P0:** Увеличить DeduplicationCacheMaxSize до 50,000 (лучше дедупликация)
3. **P1:** Увеличить BatchSize до 2000-4000 (меньше overhead)
4. **P1:** Убрать Interlocked в Single Consumer mode (меньше overhead)
5. **P2:** Вынести TickData в общую модель
6. **P3:** Добавить метрики времени записи
7. **P3:** Memory pooling для батчей
