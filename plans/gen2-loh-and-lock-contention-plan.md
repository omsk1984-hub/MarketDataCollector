# План: Gen2/LOH (5 Gen2, LOH ~20.6 MB, фрагм. ~2.6 MB) + lock contention (+1,312)

**Источник:** [`counters-analysis_20260801_143500.md`](plans/counters-analysis_20260801_143500.md:171) — проблемы 2 и 3:
- **Gen2-сборки (5) при LOH ~20.6 MB / фрагментация LOH ~2.6 MB** — умеренное GC-давление, рискованно при росте нагрузки.
- **Lock contention +1,312 за прогон** — сохраняется стабильно (~1.3K) несколько прогонов подряд.

**Прогон:** 2026-08-01 14:29:21–14:31:32, нагрузка 1.7M тиков (binance btcusdt/ethusdt/solusdt), дропы 0, записано 97%, инвариант баланса выполнен.

---

## 1. Текущее состояние кода (что уже сделано)

### По LOH / аллокациям — оптимизации применены
- `ArrayPool<TickData>` в Collector/Writer ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:412)).
- `ReusableArrayCache<T>` (пул 8 массивов точной длины) в [`BulkInsertFastAsync(IReadOnlyList<TickData>)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446).
- Pre-allocated `NpgsqlParameter[]` — без `new` на батч ([`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:30)).
- `TickData` — `readonly record struct`, `FilteredTickSlice` без `List<T>`.
- **Активный hot path не использует `decimal.ToString`** — `prices`/`volumes` передаются как `decimal[]` (строка 474-475). String-аллокации `ToString` (строки 351-352) — только в legacy-перегрузке `IEnumerable<RawTick>`, не в hot path.

### По lock contention — план существовал, но не доведён
[`reduce-lock-contention-plan.md`](plans/reduce-lock-contention-plan.md) уже локализовал кандидатов, но contention остаётся ~1.3K. Явных `lock`/`Monitor` в коде нет — только `Interlocked.*`. Метрика фиксирует внутренние блокировки фреймворка.

---

## 2. Корректировка гипотез по источникам

⚠️ **Опровержение из отчёта:** contention в [разделе 9](plans/counters-analysis_20260801_143500.md:172) приписан `DeduplicationCache` и батч-каналу. Это **неверно**:
- `DeduplicationCache` — локальный объект single consumer ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:665)), используется только в потоке одного consumer (`UseSingleConsumer=true`, один канал index=0). Конкуренции за него нет — он не thread-safe и не расшарен.
- Реальные кандидаты: **входной канал** (`Channel<TickData>` с `SingleWriter=false`, 3 WS-продюсера btcusdt/ethusdt/solusdt пишут в один канал) и **OTel-агрегаторы** (`Counter<long>.Add` на каждый тик из 3 потоков через внутренний lock `AggregatorStore`).

---

## 3. Результаты диагностики (выполнено в Code)

> Инструменты доступны: `dotnet-trace 9.0`, `dotnet-gcdump 9.0`. Примечание: версия `dotnet-trace report` в этой среде поддерживает только команду `topN` (CPU) — команда `analyze -profile-type GCAllocationTick` недоступна. Топ-типы по размеру получены из gcdump.

### Шаг A (nettrace) — топ функций по inclusive CPU
`dotnet-trace report ... topN --inclusive -n 40` дал топ CPU-потребителей:

| Ранг | Функция | Inclusive |
|---|---|---|
| 15 | `HttpConnection+ReadBufferedAsyncCore` | 31.3% |
| 20 | `WebSocketMessageReceiver.RunLoopCoreAsync` | 30.3% |
| 21 | `WebSocketConnectionManager.ReceiveAsync` | 30.3% (excl 10.2%) |
| 34 | `ArrayPool<byte>.Rent` | 12.8% |
| 35 | `TickData].WaitToReadAsync` (канал) | 9.9% |
| 36 | `OtlpLogExporter.Export` | 8.9% |
| 37 | `Task.WhenAny` | 8.6% |
| 40 | `RedirectHandler.SendAsync` (OTel HTTP) | 8.0% |

**Вывод:** значительная CPU-нагрузка идёт на WS-приём (`WebSocketConnectionManager`/`ManagedWebSocket`), `ArrayPool<byte>.Rent` (буферы чтения), ожидание канала и **OTel-экспорт** (`OtlpLogExporter` + `RedirectHandler`). CPU-картина не указывает на hot path записи в БД как узкое место.

### Шаг B (gcdump peak vs drained) — топ-типы по размеру
- **Peak heap ~31.3 MB → drained ~27.7 MB** (survivor ratio высокий, ~88% держится).
- **`System.Byte[] (Bytes > 1M)`: 5→6 объектов по ~1.05 MB** — **главный LOH-источник** (~1 MB × N, Npgsql-буферы сериализации параметров `ExecuteSqlRawAsync`). Фрагментация LOH ~2.6 MB накапливается из больших `byte[]` LOH-аллокаций на каждый батч.
- **`Entry<DedupKey,Byte>[]` (485 KB) + `DedupKey[]` (320 KB)** — резидентный кэш дедупликации, Gen2 survivor, держится весь прогон (ожидаемо, ~0.8 MB).
- **`TickData[] (Bytes > 100K)`: 18→9** — пул ArrayPool освобождается при дренаже (не утечка).
- `MetricPoint[]` (144 KB, 68) — OTel-агрегаторы.

**Вывод:** Gen2/LOH-давление подтверждённо идёт от **Npgsql `byte[]` LOH-буферов**, а не от собственного кода (пулы/struct применены, `decimal.ToString` вне hot path). Кэш дедупликации — стабильный Gen2-резидент, но малый (~0.8 MB).

### Шаг C (contention trace) — собран, разбор по стекам требует cpu-sampling
Contention-трасса **собрана** в отдельном прогоне 1.7M: [`contention_20260801_115121.nettrace`](traces/contention_20260801_115121.nettrace) (12.4 MB, `ContentionKeyword=0x4000`, Verbose, 45с на пике).
- `dotnet-trace report ... topN` вернул **«No method calls found»** — трасса собрана только с `--clrevents contention`, а `dotnet-trace report` (эта версия 9.0) агрегирует только **CPU-стеки**. Для стеков по contention нужен прогон с **contention + cpu-sampling** одновременно.

**Анализ кода (важное уточнение по источнику contention):**
- ✅ Hot-path телеметрия **уже полностью батчеризуется** через [`CounterBatcher`](src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs:20): `TicksIncoming`/`WsMessagesReceived`/`TicksDropped` инкрементятся `Interlocked.Increment` (zero-lock), реальный `Counter.Add` — раз за батч в `FlushMetricBatchers()` ([`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:281)).
- ✅ Остальные счётчики (`TicksReceived`/`TicksProcessed`/`TicksDeduplicated*`/`ExceptionsByType`) вызывают `.Add` ~660 раз за прогон (по числу батчей) — **не источник 1.3K contention**.
- **Вывод:** остаточный contention +1,312 идёт **не от OTel-счётчиков**, а от:
  1. **входного канала** `Channel<TickData>` (`SingleWriter=false`, 3 WS-продюсера btcusdt/ethusdt/solusdt пишут в один канал `TryWrite`, [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189)) — главный кандидат, согласуется с CPU-профилем (WS-приём ~30%);
  2. **OTel-экспорта** (`OtlpLogExporter` + `RedirectHandler` ~17% в topN) — внутренние lock в экспорт-пайплайне;
  3. **DI `CreateScope()`** на каждый батч ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:890)) — незначительно.

**Правило принятия:** применять только **один целевой вариант**, подтверждённый фактическим источником. Для подтверждения по стекам нужен прогон `contention + cpu-sampling`.

---

## 4. Решения по Gen2/LOH (после шага A/B)

### Вариант 1 — доминирует Npgsql `byte[]` LOH
- Переиспользовать `byte[]` буферы сериализации Npgsql (при возможности через кастомный Npgsql-конвертер или пул) — сократить LOH-фрагментацию от повторных больших аллокаций на батч.
- Увеличить батч аккуратно не стоит (уже 2500, стабильно) — вместо этого смотреть пул буферов.
- **Компактинг:** включить `GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce` при финальной фазе/высокой фрагментации — **только если фрагментация LOH > 15%** (сейчас 12.6%, порог не пройден → компактинг пока не нужен).

### Вариант 2 — доминируют string-аллокации
- Проверить WS-парсинг (`JsonDocument` уже применён) и телеметрию на лишние `string` в hot path.
- Агрегировать телеметрию батчами (см. раздел 5).

### Вариант 3 — компактинг при росте нагрузки
- Добавить фоновый мониторинг LOH-фрагментации; при превышении порога — запланированный `CompactOnce`.

---

## 5. Решения по lock contention (обновлено по фактам)

> **Статус после диагностики:** OTel-счётчики **уже батчеризованы** (`CounterBatcher`), остаточные счётчики `.Add` ~660 раз/прогон — не источник. Поэтому **Вариант A отменяется** как неактуальный. Главный источник — входной канал (3 WS-продюсера → один `TryWrite`), вторичный — OTel-экспорт.

### Вариант B (приоритет) — доминирует входной канал (3 продюсера → один канал)
- Разделить продюсеров по **per-symbol каналам** (btcusdt/ethusdt/solusdt), каждый `SingleWriter=true`, с маршрутизацией в Collector — убрать конкуренцию трёх потоков за один `TryWrite`.
- Текущая схема: `SingleWriter=false`, 3 WS-потока пишут в `channels[0].Writer.TryWrite` ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189)). Канал создан как `SingleReader=false, SingleWriter=false` (строки 140-145) даже в single-consumer mode.
- При `UseSingleConsumer=true` достаточно **одного канала с `SingleWriter=true`**? Нет — 3 продюсера по-прежнему пишут в него. Решение: **per-symbol каналы** либо **lock-free очередь** (недоступно в стандартном Channels).
- Средняя сложность: переработка `ProcessTickAsync` (роутинг по символу) + Collector (читать из N каналов).

### Вариант C — снизить OTel-экспорт (вторичный источник)
- `OtlpLogExporter` + `RedirectHandler` ~17% CPU → контеншен в экспорт-пайплайне.
- Снизить частоту экспорта логов/метрик (batch-экспорт, увеличение интервала) или отключить необязательные экспортёры (OtlpLogExporter) в прогонах.
- Низкая сложность, безопасно.

### Вариант D — DI `CreateScope()` (минимальный вклад)
- Переиспользовать один `IServiceScope`/репозиторий в рамках Writer-цикла вместо `CreateScope()` на каждый батч (~660 раз/прогон) (осторожно: thread-safety контекста).
- Низкий приоритет.

---

## 6. Критерии завершения

- Повторный прогон 1.7M (режим `all`, 90с) при той же нагрузке.
- **Gen2/LOH:** аллокации ниже ~11.3 MB/с; Gen2 ≤ 5 или фрагментация LOH не растёт при том же throughput; компактинг применён только если LOH-фрагментация перешагнула 15%.
- **Lock contention:** заметно ниже 1,312 (цель — кратно ниже), без роста дропов выше 0, без роста времени батчей и аллокаций.
- Не деградировали: запись 97%, дренаж канала 100%, инвариант баланса, пик батч-очереди ≤ ~5.

---

## 7. Вне объёма (не трогаем)

- **Нулевые дропы и инвариант баланса** — стабильны 4 прогона; схему каналов/дедупликации не менять без необходимости.
- Рост исключений (+19) — отдельный сигнал, в этом плане не рассматривается (требует проверки логов на тип исключений).
- `decimal.ToString` в legacy-перегрузке — вне hot path, не оптимизируем.
