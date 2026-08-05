# План улучшения: снижение LOH/Gen2 и потерь канала (прогон 20260805_143424)

**Источник:** [`counters-analysis_20260805_143424.md`](plans/counters-analysis_20260805_143424.md) + бинарный разбор `snapshot_peak/drained_20260805_143424.gcdump` (`dotnet-gcdump report`).

**Прогон:** 05.08 14:34, 1.7M тиков, batchSize=2500, ChannelCapacity=150000, DropOldest. Дропы **20 336 (1.20%)**, записано 95.85%, LOH heap ~49.5 MB, Gen2=2.

---

## 1. Результаты разбора gcdump (peak vs drained)

| Параметр | Peak | Drained | Δ |
|---|---|---|---|
| GC Heap bytes | 49 366 581 | 48 977 648 | **−388 933 (−0.8%)** |
| GC Heap objects | 63 546 | 62 284 | −1 262 |
| **Survivor ratio (куча)** | — | **~99.2%** | — |

### Топ LOH-типов (объекты > 85 KB)

| Тип | Peak bytes | Peak count | Drained bytes | Drained count | Примечание |
|---|---|---|---|---|---|
| **`TickData[]` (Bytes > 10M)** | **14 680 088** | 1 | **14 680 088** | 1 | **внутренний массив входного канала** `Channel<TickData>` (262 144 × 56 B = 2^18), **стопроцентно резидентный** |
| `System.Byte[]` (>1M) | 1 048 600 | 5 | 1 048 600 | **6** | Npgsql-буферы соединения (пул, не transient) |
| `TickData[]` (>100K) | 229 400 | 48 | 229 400 | 46 | **батчи записи** — дренируются слабо (46/48) |
| `System.Byte[]` (>100K) | 170 024 | 1 | 340 024 | 1 | вырос |
| `Entry<DedupKey,Byte>[]` | 484 968 | 1 | 484 968 | 1 | **DedupCache — 100% survivor** |
| `DedupKey[]` | 320 024 | 1 | 320 024 | 1 | **DedupCache — 100% survivor** |
| `MetricPoint[]` (OTel) | 144 168 | 68 | 144 168 | 68 | OpenTelemetry метрики |
| `ValueTuple<Size,Object>[]` (Npgsql) | 65 560 | 17 | 65 560 | 7 | буферы сериализации параметров — дренируются |

### Выводы из дампов

1. **⚠️ Главный резидент: `TickData[]` 14.68 MB (30% кучи)** — это внутренний буфер bounded `Channel<TickData>`. В прошлом deepdive (прогон 000308) он был 7.34 MB (2^17 = 131 072 эл.). В текущем — **14.68 MB (2^18 = 262 144 эл.)**, т.е. resize канала вырос в 2× из-за более полного канала (были дропы, канал забивался). **Это и есть +~14 MB прироста LOH 25 → 49 MB.**
2. **`System.Byte[]` ×5–6 (~1.05 MB)** — резидентные Npgsql-буферы соединения, не transient-аллокации на батч (иначе были бы тысячи, а не ~6).
3. **Транзиентная LOH-фрагментация** (8.58 MB ранее, тут ~7.1 MB) видна косвенно — snapshot живы только резиденты, транзиенты GC'нуты, но оставляют фрагментацию.
4. **Survivor ~99.2%** — куча почти не дренируется, т.к. весь живой объём — резидентные массивы (входной канал + DedupCache + Npgsql-пул).

### Сопоставление с метриками

- `gc_heap_size_loh` (49 548 272) ≈ GC Heap bytes по gcdump (49 366 581) → **практически вся живая куча находится в LOH**.
- Аллокации ~746 байт/тик — горячие источники: `decimal[]/string[]/Guid[]` уже кэшируются в [`BulkInsertFastAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:453), значит основной вклад — **парсинг входных тиков** (string/JsonDocument) и OTel-теги на пути WS→Channel → батч.

---

## 2. План изменений (по приоритету)

### 🔴 P1. Устранить потери канала (20 336 тиков) + снять LOH-давление входного канала

**Проблема (две связанные):**
- Потери: продюсер обгонял консьюмера при полностью занятом `Channel<TickData>` (DropOldest).
- LOH: внутренний массив канала resize's до 2^18 = 262 144 (14.68 MB) — единственный живой объект 30% кучи + провоцирует Gen2/LOH-проверки.

**Действия:**

1. **Ввести обратное давление вместо DropOldest** (в [`MarketDataProcessor`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs), где создаётся `Channel<TickData>` и задаётся `BoundedChannelFullMode`):
   - Перейти на `BoundedChannelFullMode.Wait` с bounded capacity, либо (предпочтительно) на **unbounded capacity** для входного канала + явное ограничение через `WaitToRead`/credit-based throttle.
   - Альтернатива, сохраняющая bounded: уменьшить `ChannelCapacity` и добавить продюсер-паузу (`await writer.WriteAsync` блокирует при полном канале). **Ожидание:** дропы → 0, канал не растёт до 2^18.

2. **Избежать огромного единственного LOH-массива канала.** `Channel<TickData>.WriteAsync` на bounded-канале растит один вектор. Если остаётся bounded — рассмотреть **сегментированную структуру** (несколько массивов по 8–16K) вместо одного resize'а до 2^18 (чувствительно к дизайну, оценить). Более рискованно; основная ставка — п.1 (не заполнять канал, тогда resize не случится).

**Критерий успеха:** `ticks_dropped_silently = 0`, канал в финале ~0, `TickData[]` в gcdump не превышает 2^17 (7.34 MB).

---

### 🔴 P2. Снизить Gen2/LOH-фрагментацию

**Действия:**

3. **Точечный компактинг LOH** при завершении/периодически:
   ```csharp
   GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
   ```
   Порог LOH-фрагментации (>15% от LOH: у нас 7.1/49.5 ≈ 14.3%, в прогоне 000308 было 18%) — вводить как конфигурируемый флаг, применять один раз в конце резкой фазы (не каждый прогон, т.к. компакт дорогой). **Ожидание:** снятие фрагментации и более редкие Gen2.

4. **Проверить `DeduplicationCache`** — 100% survivor ~0.8 MB целиком постоянный. Убедиться, что `DedupKey` — действительно value-type (уже), и что словарь периодически вычищается по TTL/размеру (не накапливает за весь прогон). Если кэш растёт ~линейно к концу — ввести жёсткий лимит поверх `DeduplicationCacheMaxSize`. **Ожидание:** стабилизация ~0.8 MB вместо роста.

---

### 🟡 P3. Снизить аллокационную нагрузку (~746 байт/тик)

**Действия:**

5. **Продолжить пулинг/kэширование** по [`allocation-reduction-hotpath-plan.md`](plans/allocation-reduction-hotpath-plan.md): проброс `ReadOnlyMemory<byte>` вместо `string` в [`WebSocketMessageReceiver`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs) и ручной `Utf8JsonReader` + `ParseDecimalFromUtf8` в [`BinanceWebSocketClient`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs). Это главный не-кэшированный источник. **Ожидание:** −30–50% аллокаций hot-path; Gen2 и LOH-транзиентная фрагментация снизятся.

6. **Npgsql-буферы `byte[]` ×6** — это пул соединения, менять не нужно, но учитывать при расчёте бюджета памяти.

---

### 🟢 P4. Мониторинг и подтверждение

**Действия:**

7. **Повторный прогон** `run_all_metrics.ps1` после каждого из P1–P3; сравнение по методике шага 5 ([`allocation-reduction-hotpath-plan.md`](plans/allocation-reduction-hotpath-plan.md)).

8. **Контрольные GCDUMP** — повторно `dotnet-gcdump report` на `snapshot_peak` нового прогона: ожидать `TickData[]` ≤ 2^17 (7.34 MB) и LOH heap ≤ ~25 MB.

---

## 3. Файлы для изменения

| Файл | Изменение |
|---|---|
| [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) | Режим заполнения входного канала (P1.1); при необходимости сегментация (P1.2); компактинг LOH (P2.3) |
| [`BinaryWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs) | `Utf8JsonReader` + `ParseDecimalFromUtf8` (P3.5) |
| [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs) | проброс `ReadOnlyMemory<byte>` (P3.5) |
| [`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) | жёсткий лимит размера при необходимости (P2.4) |
| Конфигурация | флаг LOH-компактинга, ChannelCapacity (P1/P2) |

---

## 4. Ожидаемый эффект (целевые значения на следующем прогоне)

| Метрика | Сейчас | Цель |
|---|---|---|
| `ticks_dropped_silently` | 20 336 | **0** |
| Записано в БД | 95.85% | ≥ 97% |
| `TickData[]` (входной канал) | 14.68 MB (2^18) | ≤ 7.34 MB (2^17) |
| LOH heap | ~49.5 MB | ≤ ~25 MB |
| Gen2 за 90 с | 2 | ≤ 1–2 (снижение давления) |
| Аллокации | ~746 байт/тик | ≤ ~450 байт/тик |
| Лок-контеншн | +1 181 | ≤ +500 |