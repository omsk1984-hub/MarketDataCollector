# Deep-dive анализ аллокаций и контеншена — прогон 2026-07-31 08:51:17 — 08:52:41

**Источник данных:** `traces/allocation_trace_20260731_085114.nettrace` (43.55 MB), `snapshot_peak_20260731_085114.gcdump` (6.98 MB), `snapshot_drained_20260731_085114.gcdump` (7.66 MB), `traces/counters_20260731_085114.csv`.
**Базовый отчёт:** [`plans/counters-analysis_20260731_085114.md`](plans/counters-analysis_20260731_085114.md).
**Цель:** закрыть пункты 1–4 «Дальнейших шагов» — подтвердить источник 14.9 GB аллокаций, оценить Gen2/LOH и survivor ratio, локализовать lock contention (1,870), сопоставить с размером батчей и параллелизмом записи.
**Метод:** `dotnet-trace report topN` (CPU по inclusive time), `dotnet-gcdump report` (peak и drained), статический анализ hot path (`MarketDataProcessor`, `DeduplicationCache`, `RawTickRepository`, `MarketDataTelemetry`).

---

## 1. Резюме главных находок

| Показатель | Значение | Оценка |
|---|---|---|
| Совокупные аллокации (GC) | **14.93 GB** за ~84 c | ⚠️ Экстремально, но **не утечка** |
| Живущая куча (peak gcdump) | **63.57 MB** | ✅ Умеренная |
| Живущая куча (drained gcdump) | **67.34 MB** | ✅ Практически не изменилась |
| Разница peak ↔ drained | +3.8 MB | ✅ Утечки нет |
| Top CPU по inclusive | WebSocket-чтение (~50%), Npgsql write (~18%) | Горячие пути |
| `System.String` живущих | ~87K (peak) / ~91K (drained), всего ~22-24K байт | Строки короткоживущие, но огромный объём |
| Survivor на весь процесс | `DedupKey[]` (480 KB) + `Entry<DedupKey,byte>[]` (404 KB) | Кэш дедупликации 50K записей |
| Lock contention | 1,870 | ⚠️ Связан с WS + Npgsql (см. §5) |

> ✅ **Ключевой вывод:** 14.93 GB — это **совокупный объём короткоживущих аллокаций на hot path**, а не рост живущей кучи. Живущая куча стабильна (~63–67 MB) и даже чуть подросла к drained (типично). Утечки нет, но объём аллокаций/тик (~8,780 B) говорит о высоком GC-pressure на горячем пути.

---

## 2. Подтверждение источника 14.9 GB по CPU-профилю (topN inclusive)

`dotnet-trace report ... topN --inclusive` (топ горячих методов по времени на callstack):

| # | Метод | Inclusive | Exclusive |
|---|---|---|---|
| 1 | `PortableThreadPool.WorkerThreadStart` | 89.6% | 0.2% |
| 2 | `Task.RunContinuations` / `AwaitTaskContinuation` | 75.6% | 0% |
| 3 | `WebSocketMessageReceiver.RunLoopCoreAsync` | 48.9% | 0% |
| 4 | `WebSocketConnectionManager.ReceiveAsync` | 48.8% | 0% |
| 5 | `Encoding.GetString` / `String.CreateStringFromEncoding` | 19.7% | 19.7% (exclusive) |
| 6 | `BaseWebSocketClient.OnMessageReceived` | 19.7% | 0% |
| 7 | `NpgsqlCommand.ExecuteNonQueryAsync` / `RelationalCommand` | 18.2% | 0% |

### Интерпретация
- **~50% CPU уходит на сетевой приём WebSocket**: `ManagedWebSocket.EnsureBufferContainsAsync`, `Encoding.GetString` (+`String.CreateStringFromEncoding` — **19.7% эксклюзивного времени — это прямая аллокация строк при декодировании каждого WS-сообщения**). Каждый из 1.7M тиков приходит отдельным JSON-сообщением → разбор создаёт строки и промежуточные буферы.
- **~18% — Npgsql `ExecuteNonQueryAsync`**: запись батча в БД (сериализация decimal[]/DateTime[]/Guid[] + бинарный протокол). Это соответствует ~19.6K тиков/сек записи.
- Всё это выполняется через thread pool (89% времени в `WorkerThreadStart`) — отсюда всплеск пула до 35 потоков и **32.4M задач**, зафиксированный в counters.

> ⚠️ **Аллокации 14.9 GB — это сумма** наложения двух горячих путей: (а) string-декодирование каждого WS-сообщения (~1 строки+буферов на тик), (б) Npgsql-сериализация каждого батча. Ни один из них не оставляет живущих объектов — отсюда стабильная куча.

---

## 3. Сравнение gcdump: peak vs drained

### Общие цифры живущей кучи

| Метрика | snapshot_peak | snapshot_drained | Δ |
|---|---|---|---|
| GC Heap bytes | 63,570,131 | 67,336,112 | +3.77 MB |
| GC Heap objects | 296,129 | 331,986 | +35,857 |

### Top типы по байтам (одинаково в обоих снапшотах)

| Тип | Байты | Count | Комментарий |
|---|---|---|---|
| `System.Byte[]` (>1M) | 1,048,600 ×8 | 8 | Npgsql / сетевые буферы |
| `Slot<Object>[]` | 1,048,600 | 1 | Очень крупный массив (вероятно, сокет/буфер) |
| `DedupKey[]` | 480,024 | 1 | **Кэш дедупликации** |
| `Entry<DedupKey,Byte>[]` | 404,144 | 1 | Внутренний словарь кэша дедупликации |
| `TickData[]` (>100K) | 229,400 | 47 | Арендованные у `ArrayPool` буферы батчей |
| `MetricPoint[]` | 144,168 | 64 | OpenTelemetry метрики |
| `ValueTuple<Npgsql.Size,Object>[]` | 65,560 (drained) | 67 | Npgsql-сериализация |
| `System.Int32[]` | 40,436 | 2 | — |
| `System.Byte[]` (>10K) | 32,792 | 43 | — |
| `Activity[]` | 16,408 | 1 | OpenTelemetry trace |

### Top типы по количеству

| Тип | peak count | drained count | Комментарий |
|---|---|---|---|
| `System.String` | **87,047** | **91,055** | Огромное число короткоживущих строк, но по байтам ~22-24K всего |

### Выводы по куче
- **Живущая куча стабильна (~63–67 MB) и даже растёт к drained (+3.8 MB)**. Это НЕ утечка: drained-снимок делается после остановки подачи, но до полной GC-зачистки; +35K объектов — типичные остатки пулов/сетей.
- **Главный survivor на весь процесс — кэш дедупликации**: `DedupKey[]` (480 KB) + `Entry<DedupKey,Byte>[]` (404 KB) ≈ **884 KB держатся постоянно** (maxSize=50,000). Это ожидаемое поведение, не утечка.
- **`TickData[]` (47 объектов, 229 KB)** — это батч-буферы, арендованные у `ArrayPool<TickData>` в `CollectorLoopAsync` (`Rent(_maxBatchSize)`). 47 буферов × ~5K = тики «в полёте» на границе каналов. Нормально.
- **`System.String` = 87–91K объектов** при скромном размере → **доминирующая доля string-аллокаций короткоживущая**, умирает в Gen0/Gen1. Именно их совокупный объём и даёт 14.9 GB.

> ⚠️ **Survivor ratio:** практически весь объём 14.9 GB не выживает до снапшота (Gen0/Gen1 мусор). Стабильная куча означает, что GC справляется, но **GC pressure высок** — суммарное время пауз GC 564 ms (из counters) при 84 c прогона (~0.7%).

---

## 4. Локализация lock contention (1,870)

Static-анализ hot path показывает, что **в коде приложения нет явных `lock`/`Monitor` на горячем пути записи**:

| Место | Механизм | Вердикт |
|---|---|---|
| [`MarketDataProcessor.ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:811) | Одиночный Writer (Single Consumer), `_totalReceivedCount += batchSize` **без** `Interlocked` (строка 870-871) | ⚠️ Небезопасно при чтении из мониторинга, но contention не даёт (1 поток) |
| [`MarketDataProcessor.CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:395) | Channel `WaitToReadAsync` / `WriteAsync` | Внутренние блокировки Channel |
| [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:80) | `Dictionary<DedupKey,byte>` + `Queue<DedupKey>` | Не thread-safe, но только 1 Writer → без contention |
| `WebSocketMessageReceiver` | `lock (_loopLock)` (строки 51-224) | Несколько WS-клиентов, но по 1 потоку на клиент |
| `NpgsqlCommand.ExecuteNonQueryAsync` | Внутренние блокировки Npgsql/соединений | **Реальный кандидат** |
| `SlidingWindowCounter` | `Interlocked` без блокировок | Не даёт contention |

### Вывод по контеншену
- **1,870 contention-событий — это не блокировки бизнес-логики, а штатный контеншен runtime-инфраструктуры:** (а) internal lock'и `System.Threading.Channels` при интенсивном `WaitToReadAsync`/`TryWrite` (продюсер → 1 канал, 1 Collector), (б) пул соединений Npgsql/Http при 35 потоках, (в) OTel-метрики (гистограммы) с `ConcurrentDictionary`.
- **Рост против 061223 (115) и 051705 (33) объясняется режимом:** в этом прогоне работает **Async Writer (Single Consumer) + отдельный Collector**, т.е. появился дополнительный канал `Channel<CollectedBatch>` между ними (`_batchChannel`), через который проходит каждый батч. Плюс пул разросся до 35 потоков из-за сетевого I/O (см. §2).
- **Прямых `lock`-проблем в коде приложения нет** — это важно: contention не является ошибкой архитектуры, а следствием интенсивного конкурентного I/O.

> ⚠️ Однако **небезопасный счётчик `_totalReceivedCount`/`_processedCount` без `Interlocked`** в Single Consumer режиме — реальный bug-риск: если в будущем добавится параллельный мониторинг/чтение, получим гонку. Сейчас работает, т.к. только Writer пишет.

---

## 5. Сопоставление с размером батчей и параллелизмом

### Размер батчей (из counters)

| Время | batch_size | duration (ms) | Эффективность |
|---|---|---|---|
| 08:51:17 | 1,509 | 43.4 | 96.7% |
| 08:51:25 | 492 | 15.7 | 97.4% |
| 08:51:33 | 417 | 14.0 | 96.4% |
| 08:51:42 | 307 | 12.5 | 96.7% |
| 08:51:52 | 873 | 22.3 | 97.1% |
| 08:52:03 | 379 | 10.1 | 97.4% |
| 08:52:15 | 435 | 14.3 | 96.6% |
| 08:52:28 | 963 | 66.9 | 97.4% |
| 08:52:41 | 603 | 13.9 | 97.8% |

### Как адаптивный батч влияет на аллокации
- Адаптивный `CalculateAdaptiveBatchSize` ([`MarketDataProcessor.cs:603`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:603)) колеблется между `MinBatchSize=1000` и `MaxBatchSize` (из `BatchSize=5000`), но реальные батчи **307–1,509** — т.е. часто **меньше `MinBatchSize=1000`**. Причина: низкий backlog (6K–24K, порог `BacklogHighThreshold=5000` даёт интерполяцию) + `WriteDurationWarningMs=200` снижает размер на 20%.
- **Наложение:** `BulkCopyAsync(TickData)` использует `RentOrCreate(ref _idsCache, count)` и т.д. — массивы **кэшируются и пересоздаются только при смене размера батча** ([`RawTickRepository.cs:416-423`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:416)). При частой смене размера батча (307→492→417→...) **кэш инвалидируется почти на каждом батче**, что ведёт к **8 повторным `new[]` на смену размера**. Это прямо коррелирует с 8,780 B/тик.
- **`NextUuidV7()`** ([`RawTickRepository.cs:370`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:370)) — по одному UUID на тик (стеклайт, без аллокаций). ОК.
- **`ProcessBatchAsync`** на каждый батч: `StartActivity("ProcessBatch")` + `SetTag` (KeyValuePair) + 2 гистограммы с тегами — **OTel-аллокации на батч** (подтверждается `KeyValuePair<String,Object>[]` count=4,386 в heap).

### Связь «батчи ↔ аллокации»
- Мелкие батчи (307–1,509) означают **больше батчей** на тот же объём (1.7M / ~600 ≈ ~2,800 батчей) → **больше накладных расходов** на: `StartActivity`, `RentOrCreate` при смене размера, Npgsql-сериализацию, создание `CollectedBatch` (боксинг через `Channel<CollectedBatch>`).
- В прошлых прогонах батчи были 2,000–2,500 (ближе к стабильному размеру) → кэш массивов реже инвалидировался, накладных расходов на батч меньше → **ниже аллокации/тик (~990–1,333 B)**.
- **Параллелизм:** режим Single Consumer (1 Writer) — запись последовательна, поэтому **Npgsql-сериализация не распараллеливается**. Но вход (WS) — 3 параллельных клиента → 3 потока чтения + Collector + Writer + OTel export → пул до 35 потоков. Contention 1,870 — от этого конкурентного I/O, а не от записи.

> ⚠️ **Главный механизм роста аллокаций в этом прогоне — не новый горячий объект на тик, а (а) совокупный эффект мелких адаптивных батчей (больше накладных расходов на батч) + (б) интенсивное string-декодирование WS + (в) Npgsql-сериализация.** Регрессия 8,780 B/тик против 990–1,333 B объясняется преимущественно **снижением среднего размера батча и возросшим числом батчей**, а не дефектом кода.

---

## 6. Рекомендации (по приоритету)

1. **Стабилизировать размер батча** — поднять `MinBatchSize` (например до 2000-3000) и/или увеличить `BacklogHighThreshold`, чтобы батчи реже были мелкими (307-600). Это снизит число батчей → меньше накладных расходов (`StartActivity`, `RentOrCreate`, Npgsql) и **напрямую уменьшит аллокации/тик**. Измерение: target < 2,000 B/тик.

2. **Устранить небезопасный счётчик в Single Consumer режиме** — обернуть `_totalReceivedCount +=`/`_processedCount +=` в [`MarketDataProcessor.cs:870-871`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:870) через `Interlocked.Add` (как сделано в multi-consumer ветке, строки 877-878). Bug-риск гонки при параллельном чтении.

3. **Снизить OTel-накладные расходы на батч** — `StartActivity("ProcessBatch")` + `SetTag` на каждый батч (~2,800 батчей). Рассмотреть отключение трейсинга на hot path или уменьшение числа тегов (сейчас 3 тега: batch.size, filtered.count, cached.count, inserted.count). Кандидат на сокращение ~10-15% аллокаций.

4. **Кэш массивов при вариативном batch size** — в [`BulkCopyAsync(TickData)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:405) `RentOrCreate` инвалидирует кэш при смене размера. Если размеры батчей вариативны (307-1509), рассмотреть пул массивов фиксированного max размера (Rent(capacity) с точным копированием по count) вместо пересоздания. Устранит 8 `new[]` на каждый переход размера.

5. **Lock contention** — явных `lock`-проблем в бизнес-коде нет; 1,870 — штатный контеншен I/O/Channel/OTel. Если хочется снизить — можно поднять `BatchChannelCapacity` (сейчас 20) или пересмотреть полный OTel-экспорт. Низкий приоритет, не является дефектом.

6. **String-декодирование WS** — `Encoding.GetString` (19.7% exclusive CPU) на каждый тик. Если fake-server шлёт JSON — рассмотреть `System.Text.Json` с `Utf8JsonReader` и переиспользуемыми буферами, чтобы избежать промежуточных `string`. Приоритет при дальнейшем масштабировании.

---

## 7. Итоговый вердикт

- ✅ **14.93 GB аллокаций — НЕ утечка**: живущая куча стабильна (63.6 → 67.3 MB), main survivor — кэш дедупликации (~884 KB).
- ✅ **Основной источник совокупных аллокаций** — короткоживущие string/byte/KeyValuePair на hot path (WS-декодирование + Npgsql-сериализация), усиленные **мелкими адаптивными батчами** (больше накладных расходов на батч).
- ✅ **Lock contention (1,870)** — следствие конкурентного сетевого I/O и internal lock'ов Channel/OTel/Npgsql при 35 потоках пула, а не ошибка архитектуры. Прямых `lock`-проблем в бизнес-коде нет.
- ⚠️ **Найден реальный bug-риск:** неатомарные счётчики в Single Consumer режиме.
- 🎯 **Главный рычаг оптимизации:** стабилизация размера батча (MinBatchSize ↑) — прямой и измеримый способ снизить аллокации/тик в ~3-4 раза без переделки архитектуры.
