# План исполнения: Gen2/LOH + lock contention

**Базовый план:** [`gen2-loh-and-lock-contention-plan.md`](plans/gen2-loh-and-lock-contention-plan.md)
**Источник метрик:** [`counters-analysis_20260801_143500.md`](plans/counters-analysis_20260801_143500.md:171)

**Исходное состояние:** Gen2 = 5, LOH ~20.6 MB, фрагментация LOH ~2.6 MB, lock contention +1,312 за прогон 1.7M тиков (binance btcusdt/ethusdt/solusdt). Дропы 0, запись 97%, инвариант баланса выполнен.

---

## Подтверждённые факты из кода (сверено 2026-08-01)

| Факт | Место |
|---|---|
| Входной канал в single-consumer: `SingleReader=true, SingleWriter=false` | [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:238) |
| В него пишут 3 WS-продюсера через `TryWrite` | [`ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189) |
| Batch-канал: `SingleReader=true, SingleWriter=false`, но пишет только один Collector | [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:247) |
| `CreateScope()` на каждый батч | [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:890) |
| Три `AddOtlpExporter` (metrics/tracing/logging) + runtime + EF instrument | [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:47) |
| Принудительный LOH CompactOnce каждые 5 минут (вне правила плана) | [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:22) |
| Hot-path OTel-счётчики уже батчеризуются zero-lock | [`CounterBatcher.cs`](src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs:43) |
| Главный LOH-источник — Npgsql `byte[]` (5→6 × ~1.05 MB) из `ExecuteSqlRawAsync` | [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) |

**Ключевой вывод из кода:** в single-consumer mode в **batch-канал** пишет только один Collector-поток → он может быть `SingleWriter=true` без какой-либо переработки логики. Это low-risk снижение contention, применяется сразу.

---

## Этапы

### Этап 0 — Подтверждение источника contention (обязателен до Варианта B)
Контеншен +1,312 остаётся стабильным, но стеки по contention ещё не получены (предыдущая трасса собрана только с `ContentionKeyword`, без cpu-sampling — `dotnet-trace report topN` вернул «No method calls found»).

**Действие:**
1. Собрать трассу с **contention + cpu-sampling одновременно** (как отмечено в разделе 3 Шаг C базового плана).
2. Разобрать стеки по contention → подтвердить/опровергнуть входной канал как главный источник.

**Правило принятия:** если стеки подтверждают входной канал → Вариант B (per-symbol). Если стеки указывают на OTel-экспорт → только Вариант C. Если канал подтверждён частично (шум от OTel значителен) → C + B.

---

### Этап 1 — Low-risk: batch-канал `SingleWriter=true` (без переработки)
В single-consumer mode пишет ровно один Collector ([`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:408)), поэтому `SingleWriter` можно переключить без изменений в потребителе.

**Правка:** в блоке `_useSingleConsumer` ([`MarketDataProcessor.cs:247-253`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:247)) заменить `SingleWriter = false` → `SingleWriter = true`.

> Много-consumer mode не затрагивается (там каждый Collector имеет свой канал и использует legacy `ProcessBatchesAsync`, batch-канал не создаётся).

---

### Этап 2 — Вариант C: снижение OTel-экспорта (~17% CPU, вторичный источник)
`OtlpLogExporter` + `RedirectHandler` дают ~17% в topN CPU ([`Program.cs:47-66`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:47)).

**Действия (по возрастанию риска, применять осознанно):**
1. **Настройка batch-экспорта метрик/трейсов:** `OtlpExporterOptions.BatchExportProcessorOptions` — увеличить `MaxExportBatchSize` и `ScheduledDelayMilliseconds`, чтобы меньше HTTP-запросов к OTLP-endpoint.
2. **Снизить частоту экспорта логов:** для `OtlpLogExporter` увеличить интервал (через `BatchExportProcessorOptions` в `AddOpenTelemetry` для логов).
3. **Опционально:** отключать `OtlpLogExporter` (или весь OTLP) в прогонах с нагрузкой через конфиг (`OpenTelemetry:Enabled`), оставляя только Prometheus-экспорт метрик ([`AddPrometheusExporter`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:52)).
4. Увеличить `ProcessBatchTraceSampling` (сейчас 10) — уже снижает число спанов; при необходимости до 100 на нагрузочных прогонах.

> Не отключать `AddRuntimeInstrumentation`/`AddEntityFrameworkCoreInstrumentation` полностью — они нужны для анализа, но можно вынести за флаг.

---

### Этап 3 — Вариант B: per-symbol каналы (целевой, при подтверждении канала)
Убрать конкуренцию трёх WS-продюсеров за один `TryWrite` ([`ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189)).

**Схема:**
- Вместо одного канала на consumer — **канал на символ** (btcusdt/ethusdt/solusdt), каждый с `SingleWriter=true` (в один символьный канал пишет ровно один WS-клиент).
- `ProcessTickAsync` маршрутизирует по `ticker` → `channelBySymbol[ticker].Writer.TryWrite` (карта `Dictionary<string, Channel<TickData>>`).
- Collector-сторона в single-consumer mode: либо один Collector читает из N каналов через `Task.WhenAny`/`SelectAsync`, либо N Collector'ов (по одному на канал), каждый со своим batch-каналом → один Writer.

**Правки:**
- [`ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:167) — маршрутизация по символу.
- Создание каналов в [`StartProcessingAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:233) — per-symbol `SingleWriter=true`.
- `CollectorLoopAsync`/`WriterLoopAsync` — чтение из N каналов.
- Шатдаун [`StopProcessingAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:303) — завершить все символьные каналы.

**Сложность:** средняя (переработка роутинга и Collector). **Риск:** рост дропов/деградация схемы каналов — нарушить нельзя. Поэтому применять **только после Этапа 0** и после замера Этапа 1/2.

---

### Этап 4 — Вариант D (низкий приоритет): переиспользование scope в Writer
`CreateScope()` на каждый батч ([`MarketDataProcessor.cs:890`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:890)) ~660 раз/прогон.

**Правка:** в `WriterLoopAsync` создать один scope на весь цикл и переиспользовать `IRawTickRepository`. **Осторожно:** `IRawTickRepository` — Scoped ([`DependencyInjection.cs:33`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/DependencyInjection.cs:33)); в single-consumer mode используется один Writer-поток, поэтому переиспользование scope в одном потоке безопасно. Проверить, что внутри нет per-call состояния, требующего нового scope.

---

### Этап 5 — LOH-компактинг по правилу плана
Сейчас принудительный `CompactOnce` каждые 5 минут ([`Program.cs:22-37`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:22)) — это противоречит правилу базового плана «компактинг только если LOH-фрагментация > 15%» (сейчас 12.6%).

**Правка:** убрать безусловный `CompactOnce` по таймеру; добавить пороговую проверку фрагментации LOH (>15%) перед `CompactOnce`. Либо вынести за конфиг-флаг, чтобы компактинг не вмешивался в стабильный прогон без необходимости.

---

### Этап 6 — Пул Npgsql `byte[]` LOH-буферов (главный LOH-источник)
LOH ~20 MB формируется из `byte[]` ~1.05 MB × N на каждый батч в [`BulkInsertFastAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446).

**Действия:**
1. Проверить, можно ли переиспользовать буферы сериализации Npgsql (кастомный конвертер параметров или пул `byte[]`).
2. Альтернатива: снизить размер крупных `byte[]`-аллокаций, разбивая сериализацию параметров на буферы < 85 KB (за порог LOH), либо пул больших буферов.
3. Оценить через gcdump до/после: цель — сократить LOH-фрагментацию и число LOH-объектов.

> Увеличение батча (сейчас 2500) не рассматриваем — стабильно, и рост размера параметров усугубит LOH.

---

## Критерии завершения (из базового плана, раздел 6)

- Повторный прогон 1.7M (режим `all`, 90с) при той же нагрузке.
- **Gen2/LOH:** аллокации ниже ~11.3 MB/с; Gen2 ≤ 5 или фрагментация LOH не растёт; компактинг применён только если фрагментация > 15%.
- **Lock contention:** заметно ниже 1,312 (цель — кратно ниже), без роста дропов выше 0, без роста времени батчей и аллокаций.
- Не деградировали: запись 97%, дренаж канала 100%, инвариант баланса, пик батч-очереди ≤ ~5.

---

## Порядок применения (приоритет по риску)

1. **Этап 1** (batch-канал `SingleWriter=true`) — низкий риск, мгновенный замер.
2. **Этап 0** (подтверждение канала по стекам) — обязателен до Варианта B.
3. **Этап 2** (OTel-экспорт) — низкий риск, снижает CPU и вторичный contention.
4. **Этап 3** (per-symbol каналы) — только если Этап 0 подтвердил канал; после замеров 1–2.
5. **Этап 4** (scope) — низкий приоритет, после 1–3.
6. **Этап 5 + 6** (LOH) — независимы, параллельно с 1–4.

---

## Вне объёма (не трогаем)

- Нулевые дропы и инвариант баланса — стабильны; схему дедупликации и `CounterBatcher` не менять.
- Рост исключений (+19) — отдельный сигнал, в этом плане не рассматривается.
- `decimal.ToString` в legacy-перегрузке — вне hot path.
