# План: Внедрение переиспользуемых буферов (ArrayPool-эквивалент) для batch-массивов

**Источник:** [`plans/counters-analysis-20260730-165052.md`](plans/counters-analysis-20260730-165052.md:143) — пункт 3 рекомендаций

---

## 1. Текущая ситуация

### MarketDataProcessor (уже оптимизирован)
- [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:397): `ArrayPool<TickData>.Shared.Rent(_maxBatchSize)` ✅
- [`WriterLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:561): `ArrayPool<TickData>.Shared.Return(...)` ✅
- [`ProcessBatchesAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:626): `ArrayPool<TickData>.Shared.Rent(...)` ✅

### RawTickRepository.BulkCopyAsync (НЕ оптимизирован)
В [`BulkCopyAsync(IReadOnlyList<TickData>)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:379) каждый вызов создаёт **8 новых массивов**:

| Массив | Тип | Размер (count=2500) |
|---|---|---|
| `ids` | `Guid[]` | 40,000 bytes |
| `tickers` | `string[]` | 20,000 bytes (ref) |
| `prices` | `decimal[]` | 40,000 bytes |
| `volumes` | `decimal[]` | 40,000 bytes |
| `timestamps` | `DateTime[]` | 20,000 bytes |
| `exchanges` | `string[]` | 20,000 bytes (ref) |
| `receivedAts` | `DateTime[]` | 20,000 bytes |
| `normalizeds` | `bool[]` | 2,500 bytes |

**Итого:** ~202,500 bytes на батч. При ~16 батчах/сек → **~3.2 MB/сек аллокаций** только этих массивов.

### Почему не ArrayPool напрямую
Npgsql использует `Array.Length` как количество элементов. `ArrayPool.Rent(count)` может вернуть массив **больше** запрошенного размера, что приведёт к вставке лишних default-значений (null, 0, false).

---

## 2. Решение: кэшированные переиспользуемые буферы (Reusable Buffers)

### Принцип
Вместо создания `new[]` на каждый вызов, храним массивы в полях экземпляра `RawTickRepository`. При первом вызове (или при изменении размера) создаём массив нужной длины. Все последующие вызовы переиспользуют существующий массив.

```csharp
// Поля класса
private decimal[]? _pricesCache;

// В методе
if (_pricesCache == null || _pricesCache.Length != count)
    _pricesCache = new decimal[count]; // аллокация только при изменении размера

// Заполняем _pricesCache[0..count)
// Передаём в Npgsql — Length == count, всё корректно
```

### Преимущества перед ArrayPool
| | `new[]` per-batch | ArrayPool | Reusable Buffers |
|---|---|---|---|
| Аллокаций на steady-state | 8/батч | 0 (из пула) | **0** |
| Зависимость от Npgsql Array.Length | ❌ нет | ❌ да (лишние эл-ты) | **✅ нет** |
| Сложность | низкая | средняя | **низкая** |
| Возврат в пул | N/A | требуется | **не требуется** |

---

## 3. Изменения

### 3.1. `RawTickRepository.cs` — добавить поля-кэши

```csharp
// Reusable arrays for BulkCopyAsync(TickData) — zero allocations on steady state.
// Safe because RawTickRepository is Scoped + consumer processes batches sequentially.
private Guid[]? _idsCache;
private string[]? _tickersCache;
private decimal[]? _pricesCache;
private decimal[]? _volumesCache;
private DateTime[]? _timestampsCache;
private string[]? _exchangesCache;
private DateTime[]? _receivedAtsCache;
private bool[]? _normalizedsCache;
```

### 3.2. `BulkCopyAsync(IReadOnlyList<TickData>)` — заменить `new[]` на проверку кэша

```csharp
// Вместо:
var ids = new Guid[count];
var tickers = new string[count];
// ...

// Станет:
var ids = RentOrCreate(ref _idsCache, count);
var tickers = RentOrCreate(ref _tickersCache, count);
// ...
```

### 3.3. Helper-метод `RentOrCreate` (или inline)

```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
private static T[] RentOrCreate<T>(ref T[]? cache, int count)
{
    if (cache == null || cache.Length != count)
        cache = new T[count];
    return cache;
}
```

---

## 4. Оценка эффекта

| Метрика | До | После |
|---|---|---|
| Аллокаций массивов/батч | 8 | **0** (≤1 при изменении batchSize) |
| Gen2 GC (за 73 сек) | 16 | ~5-7 (снижение за счёт общего уменьшения allocation rate) |
| Allocation rate | ~126 MB/s | ~120-122 MB/s (снижение на ~3-4 MB/s) |

---

## 5. Риски

1. **Thread safety**: `RawTickRepository` — Scoped, consumer'ы обрабатывают батчи последовательно (один writer loop). ✅ Безопасно.
2. **Утечка массивов**: Массивы живут весь lifetime репозитория (Scoped). При типичной работе worker'а это несколько минут. Если worker живёт долго (часы) — массивы одного максимального размера висят в памяти. При batchSize=2500 это ~202KB — незначительно.
3. **Изменение размера**: При адаптивном batchSize массивы пересоздаются при каждом изменении `count`. Это происходит редко (при смене нагрузки).

---

## 6. Альтернативы (отвергнуты)

| Альтернатива | Причина отклонения |
|---|---|
| `ArrayPool<T>` напрямую | Npgsql требует точного `Array.Length` |
| `ArrayPool<T>` + копирование в `new[]` | Две аллокации вместо одной — хуже |
| `GC.AllocateUninitializedArray<T>(count)` | Убирает zero-init, но всё равно аллокация per-batch |
| Оставить как есть | Не решает проблему Gen2 GC |
