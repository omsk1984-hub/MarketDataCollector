# План устранения остатка P1 (накладная вокруг INSERT) и подтверждение P2

**Дата:** 13.09.2026 · Основание: [`plans/counters-analysis_20260913_035508.md`](counters-analysis_20260913_035508.md) (после A1: batch avg 949→764 мс, но хвосты ~2.5 с на уровне `ProcessBatch`, при том что чистый insert ≤432 мс).

---

## Диагноз (из контрольного прогона 035508)

| Симптом | Источник |
|---|---|
| Чистый `BulkInsert` ≤ **432 мс** (лог A2 `tail=False` везде) | INSERT быстр ✅ |
| Включающая длительность `ProcessBatch` avg **764 мс** / max **2 551 мс** | **накладная вокруг insert** в ~×2 |
| Контеншн +2 091, задачи пула ~19.9К/с | Writer делит общий ForkJoin-пool с WS-приёмом + metrics + OTLP-экспортом |
| CTC ≈53% в topN `gc-verbose` | P2: **вероятно, артефакт инструментации** (EventPipe сам создаёт CTC; `EventPipeMetadataGenerator` 6.79% в топе; flush-путь с `linkedCts` при `fills:0%` не срабатывал) |

**Корень P1 (остаток):** `_writerTask = WriterLoopAsync(...)` (строка [`MarketDataProcessor.cs:254`](../src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:254)) выполняется как обычный async — `await foreach` бежит на **общем пуле runtime**, конкурируя за треды/блокировки с приёмом WebSocket и OTLP-экспортом. Отсюда разброс длительностей.

---

## Часть 1 — P1: изоляция Writer на выделенном потоке

### C1. Запустить WriterLoopAsync на выделенном потоке (Thread)
Вместо `WriterLoopAsync(...)` на общем пуле — выделенный `Thread` с собственным `Executor`-контекстом. Это гарантирует писателю эксклюзивный тред независимо от нагрузки WS/metrics.

**Реализация (в `MarketDataProcessor`, Single-Consumer + Multi-Consumer режим):**
- Завести `Thread _writerThread = null!` (или `Task.Run(..., dedicatedExecutor)`).
- В `StartProcessingAsync`: для Single-Consumer запускать Writer на выделенном треде:
  ```csharp
  _writerTask = WriterLoopAsync(channelIndex: 0, internalToken, _deduplicationCacheMaxSize);
  _writerThread = Thread.StartVirtual(...); // или Executor с 1 dedicated тредом
  ```
- **Важно:** async-код `WriterLoopAsync` не блокирует поток — он ждёт на `ReadAllAsync` (sleep). Выделенный тред почти всё время спит, но когда батч приходит — обрабатывает его без конкуренции за пул. Цель — **изолировать от пула**, а не добавить параллелизм.
- Реализация через **`Thread.startVirtualThread` + `$awaitable`**: 
  ```csharp
  _writerVThread = Thread.startVirtualThread(() -> {
      // привести async к блокирующему: run loop и ждать
  });
  ```
  Проще и надёжнее: создать **выделенный `Executor`** в Application-слое на 1 поток (Loom `Executors.newVirtualThreadPerTaskExecutor()` или `Executors.newSingleThreadExecutor()`), передать его как `Executor` в `Task.Run(..., executor, token)`. НО: writer-цикл — это `await foreach`, он сам разматывается. `Task.Run(() -> WriterLoopAsync(), executor, token)` закрепит корневой фрейм за одним тредом пула, и все await-продолжения этого таска попадут на тот же тред — изолируя от остальных задач.
- **Остановка:** при `StopProcessingAsync` дождаться `_writerTask` и закрыть executor (закрывайте после дренажа, в `finally`).

> ⚠️ Осторожно: весь `ProcessBatchAsync` (включая Npgsql I/O) — async и сам отдаёт тред во время network-wait. Выделенный одно-тредовый executor `Task.Run` даёт стабильную привязку продолжений. При многопоточном Npgsql большого параллелизма не добьём (одна БД-транзакция), но **устраним конкуренцию за пул** — что и является целью C1.

### C2. Снизить накладные OTLP в writer-пути
- `ProcessBatchTraceSampling`: 10 → **100** (в `appsettings.json`/`LoadTest`/`Production`) — реже создавать `ProcessBatch`-activity. С учётом батча 5000 (теперь 1/про: меньше батчей), 100 батчей ≈ 100×5000 = 500K тиков между спанами — достаточно.
- Проверить `MetricFlushBatchInterval`: оставить 1 (метрики дешевле спанов); в усиление можно 20, но риск потери данных счётчиков при срыве — **оставить 1** по умолчанию, лишь задокументировать.

## Часть 2 — P2: подтверждение артефакта CTC (без изменения прод-кода)

### D1. Прогон `contention-cpu`
- Запуск: `.\start_loadtest.ps1 -TraceProfile contention-cpu -MaxTicks 1700000 -Rps 25000`.
- Ожидание: если CTC в CPU-топе нет (или мал) — **подтверждено, что topN `gc-verbose` по CTC был артефактом инструментации** → P2 закрывается без изменений кода, фиксируется в отчёте.
- Если CTC в CPU-топе останется высоким — тогда real hot path, и потребуется отдельный анализ (обычно это артефакт, т.к. flush-путь с `linkedCts` не активен при `fills:0%`).

> Данный шаг — **нагрузочный тест**, выполняется только по явному согласию пользователя (правило проекта).

---

## Принятие решения и риски

| Решение | Действие | Риск |
|---|---|---|
| C1 (выделенный executor writer) | Изменение `MarketDataProcessor` | Средний: изменение модели исполнения async-writer. Митигируется тем, что `await foreach` корректно разматывается; регрессионный контроль — `dotnet test` (280 тестов, включая `MarketDataProcessorTests`) |
| C2 (trace sampling 100) | Изменение конфигов | Низкий: меньше спанов, меньше накладных OTLP |
| D1 (прогон contention-cpu) | Только сбор данных | Нулевой (но нагрузочный — требует согласия) |

---

## Список изменяемых файлов (C1+C2)
1. `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs` — выделенный executor для Writer + shutdown
2. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json` — `ProcessBatchTraceSampling` 10→100
3. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.LoadTest.json` — то же
4. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json` — то же

## D1 (по согласию на нагрузку)
5. Запуск `start_loadtest.ps1 -TraceProfile contention-cpu` → анализ `*-topn.md` + counters.

---

## ✅ Статус выполнения (13.09.2026, решение пользователя)

**C1 реализован в упрощённой безопасной форме** — вместо выделенного executor (который в .NET/RavenDB не изолирует async-продолжения без SynchronizationContext — крупное изменение) выбран переиспользуемый scope на уровне Writer-цикла:

- [`MarketDataProcessor.cs:575`](../src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:575) — `WriterLoopAsync` создаёт **один `scope` + `IRawTickRepository` на весь цикл** и передаёт его в `ProcessBatchAsync` (раньше `CreateScope()` + `GetRequiredService<IRawTickRepository>()` выполнялись на **каждый батч** → RavenDB резолвил новый RawTickRepository + DbContext ≈1100 раз/прогон).
- [`MarketDataProcessor.cs:838`](../src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:838) — `ProcessBatchAsync` получил параметр `IRawTickRepository? reusedRepository = null`: если передан — используется (fast path, ноль scope-аллокаций), иначе создаётся scope на батч (legacy multi-consumer backward-compat).
- Безопасность: Single-Consumer Writer строго последователен (однопоточен), поэтому переиспользование репозитория/подключения корректно; `ReusableArrayCache`/`NpgsqlParameter[]` уже рассчитаны на переиспользование между батчами.

**C2 реализован** — `ProcessBatchTraceSampling` 10→100 в 3 конфигах (appsettings/ LoadTest/ Production): реже создаются `ProcessBatch`-activity (спаны), ниже накладные OTLP в writer-пути.

**D1 — отложено** (нагрузочный прогон `contention-cpu` не запускался — требует согласия; по выбору пользователя).

## ✅ Проверка
- `dotnet build` Application: **0 warning, 0 error**.
- `dotnet test`: **280 passed / 0 failed** (MarketDataProcessorTests — C1-регрессия, включая single+multi-consumer).
- JSON конфигов валиден (проверено ConvertFrom-Json).

## Критерий готовности
- `dotnet build` 0 ошибок; `dotnet test` 280 зелёные.
- Контрольный прогон: чистый insert ≤ 500 мс; включающая длительность `ProcessBatch` max < 2 с; contention в `contention-cpu` не в топ-3 (или документированно мала).