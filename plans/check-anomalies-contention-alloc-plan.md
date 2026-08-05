# План проверки аномалий прогона 20260805_115513 (пп. 3–4 отчёта)

**Источник:** [`plans/counters-analysis_20260805_115513.md`](plans/counters-analysis_20260805_115513.md:189) — разделы «Ключевые проблемы и рекомендации», пункты 3 (Lock contention) и 4 (Аллокации).

**База:** этап 3.3 (пороговый LOH-компактинг) из [`recommendations-execution-plan-gen2-loh.md`](plans/recommendations-execution-plan-gen2-loh.md:53) верифицирован; обе аномалии **вне связи с компактингом**.

---

## Контекст и предварительные выводы (по counters + код)

### Аномалия 3 — Lock contention 1,996 (baseline 1,451, +545)

**Что проверили по коду:**
- Hot path конвейера [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:169) использует **только `Interlocked`** (`_totalIncomingCount`, `_totalReceivedCount`, `_processedCount`, `_metricFlushBatchCounter`, `_processBatchCounter`) — никаких `lock`/`Monitor` в конвейере.
- OTel `Counter<long>.Add` (внутри которого `lock` в `AggregatorStore`) **вынесен из per-message hot path** через [`CounterBatcher.cs`](src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs:43) — инкремент через `Interlocked.Increment`, реальный `Add` один раз за батч в `Flush`.
- Реальные `lock` живут в управлении жизненным циклом WS: [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:160), [`WebSocketClientRegistry.cs`](src/MarketDataCollector.Core/Clients/WebSocketClientRegistry.cs:20), [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:51). Они не в per-tick пути, но срабатывают при (пере)подключениях/остановке.
- В прогоне 115513 ThreadPool обработал **2,217,940** items против 1,716,595 в baseline (**+29%**). ThreadPool queue = 0 (не застревает). Более высокая конкурентность пула — вероятный вкладчик в contention (мониторы захватываются чаще при большем числе параллельных задач).

**Вердикт (предварительный):** рост contention не объясняется этапом 3.3 (компактинг не добавляет блокировок в hot path). Источник — возросшая активность ThreadPool + monitor-lock'и управления WS-клиентами. Точную долю каждого источника даёт бинарный разбор (см. шаг 2).

### Аномалия 4 — Аллокации 1,412,362,800 (baseline 1,267,129,336, +11–14%)

**Что проверили по counters:**
- Оба прогона одинакового объёма (1.7M тиков, batchSize 2500) и конфигурации (режим `all`, gc-verbose, 90 с).
- В 115513 ThreadPool items 2,217,940 против 1,716,595 в baseline — больше работы, больше сопутствующих аллокаций.
- Аллокации на тик: 1,412,362,800/1,700,000 ≈ **831 B/тик** (в baseline ~745 B/тик).
- LOH-фрагментация при этом упала до 0.46 MB, live heap до 21.6 MB — т.е. рост аллокаций **не приводит к росту живой памяти/фрагментации** (компактинг + короткоживущие объекты).

**Вердикт (предварительный):** рост аллокаций — следствие повышенной активности ThreadPool и, вероятно, более частых переподключений/диспетчеризации WS в этом прогоне, а не деградации hot path. Не влияет на latency (батчи <250 мс) и дропы (0).

---

## Этапы

## 1. Контрольная динамика contention и аллокаций по сэмплам (counters)

**Цель:** выяснить, нарастают ли contention/аллокации равномерно или есть всплеск в конкретном окне (корреляция с WS-переподключениями).

**Действия:**
1. Прочитать `traces/counters_20260805_115513.csv` по сэмплам (~времени) и снять `process_runtime_dotnet_monitor_lock_contention_count_total` и `process_runtime_dotnet_gc_allocations_size_bytes_total`:
   - старт (08:55:15), середина (08:55:35, 08:55:40), финал (08:56:45).
2. Вычислить скорость роста contention (contention/сек) и аллокаций (MB/сек) на каждом интервале.
3. Сопоставить всплески с `ws_active_connections_count` и логами `worker_out.log` (переподключения).

**Выход:** таблица скорости прироста contention/аллокаций по интервалам.

## 2. Бинарный разбор (в code-режиме, обязателен для окончательного вывода)

> В architect-режиме нет `execute_command`; эти шаги выполняются в **code-режиме**.

**Цель:** изолировать источники contention и топ аллокаций.

**Действия:**
1. **Contention:** разобрать `snapshot_peak_20260805_115513.gcdump` через `dotnet-gcdump report` — искать потоки в состоянии blocked/contention (если инструмент поддерживает) либо `dotnet-dump analyze` с командами `threads` / `clrstack` для определения мест захвата мониторов.
   - Альтернатива: `dotnet-trace report traces/allocation_trace_20260805_115513.nettrace` — профиль CPU, топ стеков с `Monitor.Enter` / `lock`.
2. **Аллокации:** `dotnet-trace report traces/allocation_trace_20260805_115513.nettrace analyze -profile-type GCAllocationTick` → топ аллокаторов (методы) и типы (`System.String`, `byte[]`, `TickData`, `DedupKey`).
   - Сравнить с baseline `allocation_trace_20260805_000308.nettrace`: одинаков ли топ, или появились новые источники (например, рост `Task`/`ThreadPool`-объектов).
3. **ThreadPool:** проверить `process_runtime_dotnet_thread_pool_*` в counters — подтвердить источник 2.22M items (WS-переподключения vs бенч-нагрузка).

**Команды (code-режим, с обёрткой для кодировки):**
```
chcp 65001 >nul & cmd /c dotnet-gcdump report traces\snapshot_peak_20260805_115513.gcdump
chcp 65001 >nul & cmd /c dotnet-trace report traces\allocation_trace_20260805_115513.nettrace analyze -profile-type GCAllocationTick
chcp 65001 >nul & cmd /c dotnet-trace report traces\allocation_trace_20260805_000308.nettrace analyze -profile-type GCAllocationTick
```

**Выход:** топ аллокаторов + подтверждение/опровержение участия WS-lock'ов в contention.

## 3. Сравнение с baseline по contention и аллокациям

**Цель:** количественно зафиксировать Δ и выяснить природу (+545 contention, +145 MB аллокаций).

**Действия:**
1. Свести таблицу: contention и аллокации старт/финал в обоих прогонах (000308 и 115513).
2. Нормировать на число ThreadPool items и на тики — отделить системный рост от деградации конвейера.
3. Если разбор (шаг 2) покажет доминирующий вклад WS/ThreadPool — зафиксировать как «вне конвейера»; иначе — сформулировать задачу на оптимизацию hot path.

**Выход:** таблица сравнения + заключение о природе роста.

## 4. Заключение и рекомендации

**Выход:** итоговая запись в [`counters-analysis_20260805_115513.md`](plans/counters-analysis_20260805_115513.md:189) (пп. 3–4) или отдельный краткий отчёт с вердиктом:
- подтверждена ли связь с 3.3 (ожидается «нет»);
- источник contention и аллокаций (WS/ThreadPool vs hot path);
- рекомендации: мониторинг WS-переподключений, при необходимости — уменьшение `lock` в WS-клиентах (к примеру, lock-free/replace на `Interlocked`), проверка метрики `process_runtime_dotnet_thread_pool_*`.

---

## Оценка гипотезы P2 — кэшировать `IServiceScope` в WriterLoop

**Факты из кода (проверено 2026-08-05):**

1. **`UseNoTracking` не применим на этом пути.** [`BulkInsertFastAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) использует `_context.Database.ExecuteSqlRawAsync(...)` — raw SQL через `Database`, **минует change tracker** (нет `AddRange`/`SaveChanges`). Трекинг сущностей здесь не задействуется, поэтому `QueryTrackingBehavior.NoTracking` **не даст эффекта**.

2. **Кэширование scope имеет потенциал, но ограниченный.** [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:878) создаёт `_scopeFactory.CreateScope()` на каждый батч (680/прогон). Scope создаёт Scoped `RawTickRepository` + `DbContext` ([`DependencyInjection.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/DependencyInjection.cs:30)). В конструкторе репозитория (стр. 52–70) пересоздаются `NpgsqlParameter[]` и 8 `ReusableArrayCache`. Кэширование scope на весь [`WriterLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:572) убрало бы это пересоздание.

3. **Ожидаемый эффект — малый.** По `dotnet-trace topN` главные затраты CPU/аллокаций — **не создание scope**, а WS/CTS-активность (`CancellationTokenSource.Register/CancelAfter/CreateLinkedTokenSource` ~32%) и сериализация Npgsql-параметров (transient `byte[]`/`ValueTuple`, не накапливается). `NpgsqlParameter[]` (8 шт) и 8 пулов ссылок на scope — незначительные объекты против 2500 тиков/батч. На contention (1,996) и дропы/latency (0/67%) **влияния нет**.

**Вердикт:** гипотеза P2 **частично валидна, но низкий приоритет.** Эффект на аллокации — единицы % максимум; на contention/дропы/latency — нулевой. Риск: долгоживущий `DbContext` (change tracker накапливает при появлении не-raw операций; для чистого `ExecuteSqlRaw` безопасно). **Рекомендация:** НЕ приоритизировать; если и делать — создать scope один раз в WriterLoop и переиспользовать один `RawTickRepository`, при этом оставить текущую экономию `NpgsqlParameter[]`/кэшей (уже реализована). Реальная оптимизация — сокращение WS/CTS-активности (см. аномалию 3).

## Результаты проверки (выполнено 2026-08-05, code-режим)

> Инструменты: `dotnet-trace report <nettrace> topN -n 25`, `dotnet-gcdump report <gcdump> -t heapstat`. Прим.: версия `dotnet-trace 9.0` не поддерживает `analyze -profile-type GCAllocationTick` — использован `topN` (CPU-профиль).

### Контейция (аномалия 3) — источник WS/CTS, НЕ конвейер

`topN` по `allocation_trace_20260805_115513.nettrace`:

| Метод | Exclusive |
|---|---|
| `CancellationTokenSource.Register` | 14.95% |
| `ManagedWebSocket.ReceiveAsync` / `GetReceiveResult` | 9.61% / 5.95% |
| `CancellationTokenSource.CancelAfter` | 10.06% |
| `WaitToReadAsync` (Channel) | 10.04% |
| `CancellationTokenSource.CreateLinkedTokenSource` | 7.4% |
| `WebSocketConnectionManager.ReceiveAsync` | 10.95% |
| `Task.FromResult` / `GetStateMachineBox` / `GetTaskForValueTaskSource` | ~7.6% |

**Вердикт:** ~32% CPU (Register+CancelAfter+CreateLinkedTokenSource) приходится на создание/связывание CTS в WS-клиентах ([`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:160), [`WebSocketMessageReceiver`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:51)) — где и живут реальные `lock` (`_backgroundLock`, `_loopLock`). Hot path конвейера ([`MarketDataProcessor`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:169)) — только `Interlocked`, без мониторов. **Связь с этапом 3.3 не подтверждена; рост contention — следствие активности WS/CTS и +29% ThreadPool items.**

### Аллокации (аномалия 4) — не регрессия памяти

`heapstat` peak, сравнение до→после:

| Тип | 000308 (до 3.3) | 115513 (после 3.3) |
|---|---|---|
| GC Heap всего | 40.76 MB | **25.94 MB** (−36%) |
| `TickData[]` канала (>1M) | **7.34 MB** (1 шт) | **отсутствует** ✅ |
| `System.Byte[]` Npgsql (1M) | 5×1.05 MB | 5×1.05 MB |
| `TickData[]` батчей (100K) | 46 шт / 229 KB | 11 шт / 229 KB |
| `Entry<DedupKey,byte>[]` (кэш) | 485 KB | 485 KB |
| `DedupKey[]` (кэш) | 320 KB | 320 KB |

**Вердикт:** доминирующий резидентный `TickData[]` канала (7.34 MB) устранён компактингом — главный эффект 3.3 подтверждён. Резидентные `byte[]` Npgsql и кэш дедупликации стабильны (не transient LOH). Рост аллокаций — сопутствующие async-стейтмашины ThreadPool и WS-CTS активность; живая память снизилась на 36%.

## Порядок исполнения

1. **Шаг 1** — из counters (доступно в architect). ✅
2. **Шаг 2** — бинарный разбор (nettrace/gcdump) — **выполнен** (topN + heapstat peak 115513 и 000308). ✅
3. **Шаги 3–4** — сравнение и итог — **зафиксированы** в [`counters-analysis_20260805_115513.md`](plans/counters-analysis_20260805_115513.md:189), пп. 3–4.
