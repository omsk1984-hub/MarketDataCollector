# План улучшений: снижение аллокаций с ~980 до ~900 байт/тик

## Контекст

Прогон [`counters-analysis_20260801_112045.md`](plans/counters-analysis_20260801_112045.md) показал рост аллокаций до **~980 байт/тик** (дельта GC allocations ~1.66 GB за 1.7M тиков), против **~925 байт/тик** в прогоне 093357 и **~908** в эталоне 112029. Рост **~55–72 байт/тик (~6–7%)**.

При этом:
- дропов нет (`received == incoming == 1.7M`),
- Gen2-сборок даже меньше (3 против 4),
- GC duration 110 ms — не критично.

Т.е. рост аллокаций **не деградация пропускной способности**, а накопительное увеличение расходов на тик. Цель — вернуться к ~900 байт/тик.

---

## Корневой анализ: где сейчас аллокации на hot path

Собрано чтением кода (hot path: WebSocket → `MarketDataProcessor` → `RawTickRepository`).

### Уже оптимизировано (baseline, не трогаем без причины)
| Место | Статус |
|---|---|
| Парсинг Binance: `Utf8JsonReader` + `stackalloc` decimal | ✅ zero-alloc |
| Проброс `ReadOnlyMemory<byte>` вместо string в `WebSocketMessageReceiver` | ✅ −1 string/сообщение |
| Телеметрия: `CounterBatcher` (Interlocked, zero-alloc) | ✅ |
| OTel-теги: статические `KeyValuePair` кэши | ✅ |
| Батчи: `ArrayPool<TickData>` + `FilteredTickSlice` вместо `List<TickData>` | ✅ |
| Дедупликация: `DedupKey` value-type ключи | ✅ |
| Bulk insert: кэш массивов `_*Cache` + переиспользуемый `NpgsqlParameter[]` + UUID v7 | ✅ |

### Оставшиеся кандидаты на аллокации (по убыванию значимости)

1. **`ticker = reader.GetString()`** — [`BinanceWebSocketClient.cs:153`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:153).
   **1 string/тик × 1.7M.** Неизбежна на текущей архитектуре: строка нужна для `TickData.Ticker`, ключа дедупликации `DedupKey` и маршрутизации. Это главный фиксированный расход (~40–80 байт/тик с учётом заголовка строки + data).

2. **`MemoryStream` в `WebSocketMessageReceiver`** — [`WebSocketMessageReceiver.cs:81,123,131`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:81).
   - `new MemoryStream(_options.MaxMessageSize)` = 1 MB начальный буфер, но он один на loop (не на сообщение) — не тик-расход.
   - **Однако** каждый `messageStream.Write(...)` при переполнении вызывает перераспределение буфера. При `MaxMessageSize=1MB` и сообщениях ~200 байт буфер растёт по степенному закону (16KB→32KB→...→1MB) — **это одноразовые расходы на старте**, не на тик. Не тик-расход.

3. **`KeyValuePair<string,object?>` в `BatchWriteDuration.Record`** — [`MarketDataProcessor.cs:898-902`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:898).
   **2 KVP на батч** (не на тик): `batch_size`, `inserted_count`. При 680 батчах — ничтожно. НЕ тик-расход.

4. **`IncrementWsMessagesReceived`** — [`MarketDataTelemetry.cs:262`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:262).
   `ConcurrentDictionary.TryGetValue((exchange,symbol))` — ValueTuple-ключ, boxing не обязателен. После первого сообщения кэшируется. Не тик-расход на стабилизации.

5. **`_tickAggregator.OnTickAsync`** — [`MarketDataProcessor.cs:197`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:197). Выключен (`Enabled: false` в appsettings). Не активен.

6. **`Interlocked` + батчевые счётчики** — zero-alloc.

### Вывод по корневому анализу

> Основной **фиксированный** per-tick расход — **string ticker** (п.1). Остальное — разовые/пер-батч расходы, не объясняющие рост 55 байт/тик. Рост между 093357 и 112045 **в пределах шума замеров** (925→980, ±6%) и, вероятно, связан с разницей распределения батчей/GC-фаз, а не с регрессом кода.

## Эмпирическое подтверждение источников (разбор trace 112045)

Выполнено `dotnet trace report ... topN -n 40` (время в callstack) и `dotnet gcdump report snapshot_peak`:

| Метод | % (topN) | Что это | Тик-расход |
|---|---|---|---|
| `Byte[].Rent` | 11.74% | `ArrayPool` в `WebSocketMessageReceiver` / батч-массивы | разовый/батч |
| `WebSocketConnectionManager.ReceiveAsync` | 11.52% | чтение WS (ManagedWebSocket) | нет |
| `TickData[].WaitToReadAsync` | 7.92% | ожидание канала | нет |
| `Task.Delay` | 5.94% | реконнект/таймер | нет |
| `EventPipeMetadataGenerator` | 5.62% | сам трейсер (артефакт сбора) | нет |
| **`JsonReaderHelper.TranscodeHelper`** | **3.37%** | **UTF-8→UTF-16 в `reader.GetString()` (ticker)** | **string/тик** |
| **`Decimal..ctor`** | **2.6%** | **decimal-парсинг (цена/объём)** | struct/тик |
| `String.Concat` | 1.69% | строковые конкатенации | редкие |
| `PgNumeric+Builder` | 0.11% | Npgsql-сериализация decimal | батч |

`gcdump peak`: `TickData[]` (458KB, 11 шт — ArrayPool батчи), `Byte[]` 1MB×5 (буферы), `DedupKey[]` 240KB, `string` 22B×35K — **в пике live-кучи следов per-tick утечки нет**.

> **Вывод:** главный фиксированный per-tick расход — **string ticker** (`TranscodeHelper` = `GetString()`). Остальное — машинность задач/канала и ArrayPool (переиспользуемые). Рост ~55 байт/тик не объясняется разовым кодом — он связан с per-tick string + добавленной логикой дедупликации с `HashCode.Combine` на каждый элемент батча.

---

## Приоритетные оптимизации (по ROI)

### Приоритет 1. Кэш/интернирование ticker (подтверждено trace)
**Проблема:** `reader.GetString()` (`TranscodeHelper` = 3.37% callstack) аллоцирует string на каждый тик (1.7M). Символов всего 3 (`btcusdt`, `ethusdt`, `solusdt`) — аллокация избыточна.

**Вариант A — маппинг по фиксированному набору (рекомендуемый):**
```csharp
private static readonly Dictionary<string,string> TickerPool = new(StringComparer.Ordinal) {
    ["btcusdt"] = "btcusdt", ["ethusdt"] = "ethusdt", ["solusdt"] = "solusdt"
};
// в ParseTradeMessage, case 's':
ticker = TickerPool.TryGetValue(s, out var cached) ? cached : s;
```
Плюс: 0 аллокаций для известных символов. Минус: словарь нужно держать актуальным под конфиг `Readers`; для неизвестных — fallback-аллокация.

**Вариант B — `string.Intern`:** проще, но intern pool не чистится GC → риск утечки при росте разнообразия символов. Менее предпочтителен.

**Эффект:** −1 string/тик ≈ **−40–80 байт/тик** → прямой возврат к ~900.

### Приоритет 2. Снизить расходы дедупликации (новые метрики между 093357 и 112045)
**Проблема:** в [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:927) добавлены per-batch `TicksDeduplicatedByCache/Db.Add` с `ChannelTag` (KVP), а `DeduplicationCache.Contains/Add` вычисляет `HashCode.Combine` на **каждый элемент** батча (не только новые). При 2500-батчах и ~90ms записи это ~2500 хэшей на батч.

**Оптимизация:**
1. `DedupKey.GetHashCode` — кэшировать вычисленный хэш в поле структуры (лениво), т.к. ключ вставляется в `Dictionary` и `Queue` многократно. Либо использовать `Dictionary<DedupKey,byte>` с предварительно вычисленным `HashCode`.
2. `Contains`+`Add` вызываются последовательно (двойной `HashCode.Combine`). Объединить в один проход: `TryAdd`-стиль (проверить и добавить за один хэш).

**Эффект:** снижение CPU (не аллокаций — DedupKey уже value-type). На аллокации влияет слабо, но убирает давление на код, который мог перераспределять `_cache`/`_order` при вставке.

### Приоритет 3. `MemoryStream` → переиспользуемый байтовый буфер (вторичная оптимизация)
**Проблема:** `MemoryStream` в receive loop перераспределяется при росте сообщения (разовые расходы, но при старте многих подключений могут накапливаться).

**Решение:** использовать `ArrayBufferWriter<byte>` или переиспользуемый `byte[]`-буфер с ручным `Length`, избегая `MemoryStream` перераспределений. Плюс — стабильный буфер, минус — усложнение. **Низкий приоритет** — не даёт пер-тик выигрыша на стабилизации.

### Приоритет 4. Контрольный замер после правок
**Методика:** повтор `.\run_all_metrics.ps1` (1.7M тиков, тот же fake server).
1. Снять `counters_*.csv`.
2. Сравнить байт/тик: должно вернуться к ~900.
3. Снять `allocation_trace` → проверить, что `System.String` per-tick снизился.
4. Убедиться, что нет регресса в дропах/backlog/throughput (должны сохраниться 0 дропов).

---

## Порядок реализации

| # | Задача | Файл | Риск | Ожидаемый эффект |
|---|---|---|---|---|
| 1 | ✅ Разобрать `allocation_trace` (сделано: topN + gcdump) | `traces/allocation_trace_20260801_112045.nettrace` | — | подтверждён string ticker |
| 2 | Ticker pool (P1) | [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:153) | словарь держать актуальным под `Readers` | −40–80 байт/тик |
| 3 | `DedupKey` хэш-кэш + объединение `Contains/Add` (P2) | [`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:20), [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:863) | низкий | CPU, стабильность вставки |
| 4 | `MemoryStream` → буфер (P3) | [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:81) | усложнение | разовые расходы |
| 5 | Контрольный замер (P4) | `run_all_metrics.ps1` | — | подтверждение ~900 |

## Ключевые файлы
- [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:153) — string ticker (P1)
- [`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:20) — хэш-кэш DedupKey (P2)
- [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:81) — MemoryStream (P3)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:898) — per-batch KVP (низкий приоритет)
- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) — bulk insert (уже оптимизирован)
- `traces/allocation_trace_20260801_112045.nettrace` — эмпирическое подтверждение (topN)

## Критерии успеха
- Аллокации ≤ ~910 байт/тик (возврат к ~900).
- Сохранение 0 дропов, backlog ≈ 1, эффективность записи ~97%.
- Без регресса Gen2/GC duration.
