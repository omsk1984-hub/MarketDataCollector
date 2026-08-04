# План: локализация lock contention (+955) в прогоне 120839

**Источник:** [`counters-analysis_20260804_120839.md`](plans/counters-analysis_20260804_120839.md:181) — Проблема 2.

---

## 0. Корректировка исходных данных (важно)

В отчёте в таблице (строка 166) `Lock contention | +1,538 | +297 | +955`, но в выводе (строка 173) написано «+955 — выше обоих прошлых». **Это внутреннее противоречие отчёта:**

| Прогон | Contention | Статус |
|---|---|---|
| 230250 (полный) | **+1,538** | завершён |
| 005557 (незавершён) | +297 | преждевременно остановлен |
| **120839 (текущий)** | **+955** | завершён |

Фактически `+955 < +1,538`: текущий прогон **лучше** завершённого 230250 и вернулся к уровню до введения батчевого сбора метрик. Рост над минимальным +297 объясним: тот прогон был **прерван** и не захватил весь контеншен хвоста.
**Вывод:** контеншен **не регрессия**, а возврат к норме завершённого прогона. Цель плана — подтвердить источник и оценить, стоит ли оптимизировать.

---

## 1. Что говорит код (диагностика уже проведена)

### 1.1 `DeduplicationCache` — НЕ источник (исключено)
[`DeduplicationCache.cs`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:63) помечен «не thread-safe, блокировки не нужны». В single-consumer режиме создаётся **один экземпляр на Writer** ([`MarketDataProcessor.cs:589`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:589)), доступ только из одного потока Writer'a. Явных `lock`/`Monitor` в нём нет — только `Dictionary`/`Queue`. **Исключен из кандидатов.**

### 1.2 Явных `lock`/`Monitor` в hot path нет
Регекс-поиск по `src` нашёл `lock` только в **WS-клиентах** (`BaseWebSocketClient`, `WebSocketMessageReceiver`, `WebSocketClientRegistry`), которых на 1.7M-прогоне 3–4. В `Application` — только `Interlocked.*`. Метрика `.NET Monitor` фиксирует **внутренние блокировки фреймворка**.

### 1.3 Кандидаты внутреннего контеншена (single-consumer, 1.7M тиков)
Конфиг: `UseSingleConsumer=true`, `ChannelCapacity=150000`, `BatchChannelCapacity=40`, `MinBatchSize=2500`, `TickAggregator.Enabled=false` ([`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:22)).

1. **Входной `Channel<TickData>`** — главный кандидат. Создан `SingleWriter=false` ([`MarketDataProcessor.cs:241`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:241)); в него пишут **3+ WS-потока** (btcusdt/ethusdt/solusdt, плюс binance2) через `TryWrite` ([`MarketDataProcessor.cs:189`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189)). При ~19K тиков/сек и backlog этот один `TryWrite` — точка конкуренции производителей. Это согласуется с выводом [`gen2-loh-and-lock-contention-plan.md`](plans/gen2-loh-and-lock-contention-plan.md:96), где OTel-счётчики уже исключены (батчеризованы).
2. **OTel-экспорт** (BatchExportProcessor / AggregatorStore / OtlpLogExporter) — вторичный, он остаётся на экспорт по расписанию.
3. **DI `CreateScope()` на каждый батч** ([`MarketDataProcessor.cs:900`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:900)) — ~680 батчей/прогон → незначительно.
4. **Npgsql/EF-контекст** при `BulkInsertFastAsync` — внутренние блокировки пула соединений/репозитория.

### 1.4 Контекст и масштаб
- 3 потока-продюсера → 1 канал `TryWrite` — типичный источник контеншена `.NET Monitor`.
- Степень контеншена (+955 за ~100с = ~9.5/с) **не создаёт квантифицируемую проблему производительности**: queue=0, дропы=0, инвариант баланса точный, дренаж 100%. Это расход CPU, не влияющий на корректность/потери.

---

## 2. Диагноз (1–2 наиболее вероятных источника)

> Наиболее вероятный источник — **входной канал `Channel<TickData>`** (3 WS-продюсера → один `TryWrite`, `SingleWriter=false`). Второй по значимости — **OTel-экспорт**. `DeduplicationCache` исключён как single-threaded.

---

## 3. Шаги верификации (подтвердить перед оптимизацией)

### Шаг A — собрать contention trace (подтвердить источник по стекам)
Прогон 1.7M с провайдером контеншена (существующий `allocation_trace_20260804_120839.nettrace` собран профилем `gc-verbose` и **не содержит contention-событий**):
```
dotnet-trace collect --process-id <pid> \
  --providers Microsoft-Windows-DotNETRuntime:0x4000:5 \
  --output traces/contention_120839.nettrace --duration 100
```
Группировать события `Contention`/`ContentionStop` по фреймам.
Ожидаемые кластеры:
- `System.Threading.Channels.Channel...TryWrite/WriteCore` → входной канал (подтверждает Вариант A);
- `OpenTelemetry`/`AggregatorStore`/`Metric` → экспорт (подтверждает Вариант B);
- `Npgsql`/`Microsoft.EntityFrameworkCore` → запись в БД.

### Шаг B — замерить вклад по стекам
Сравнить число событий и суммарное время ожидания на кластер. Оптимизировать **только** преобладающий источник (правило «одного целевого варианта» из [`reduce-lock-contention-plan.md`](plans/reduce-lock-contention-plan.md:77)).

### Шаг C — лог-подтверждение уже имеется
Отчёт не показывает исключений, связанных с каналом; `thread_pool_queue_length=0` — пул не перегружен. Если contention-провайдер недоступен, для подтверждения достаточно корреляции: рост происходит на старте (3 продюсера догоняют writer при наибольшем backlog ~43K → максимум контеншена на входном канале).

---

## 4. Варианты оптимизации (после подтверждения)

### Вариант A (приоритет) — входной канал (3 продюсера → один `TryWrite`)
- **Per-symbol каналы**: btcusdt/ethusdt/solusdt → отдельные `Channel<TickData>` с `SingleWriter=true`, маршрутизация в Collector. Убирает конкуренцию трёх потоков за один `TryWrite`.
- Сложность средняя: переработка `ProcessTickAsync` + `CollectorLoopAsync` (читать из N каналов).
- Требует ре-верификации дренажа/инварианта (не сломать нулевые дропы).

### Вариант B — OTel-экспорт (вторичный)
- Снизить частоту экспорта логов/метрик (increase batch interval / disable OpLogExporter в прогонах).
- Низкая сложность, безопасно.

### Вариант C — `DeduplicationCache` (из опций не выполнять)
- **Не требуется:** кэш single-threaded, контеншена не даёт. Per-shard lock / lock-free структуры — **неприменимы** и только добавят накладных расходов. Строки plаn о «lock-free структурах для DeduplicationCache» основаны на ошибке отчёта и подлежат отмене.

---

## 5. Критерий завершения
- [ ] Повторный прогон 1.7M (режим `all`, ~100с) при той же нагрузке с contention-провайдером.
- [ ] `lock_contention` заметно ниже ~955 (цель — кратно ниже для выбранного варианта).
- [ ] Без регрессий: дропы 0, запись 97%, дренаж 100%, инвариант баланса, аллокации ≤ ~12.5 MB/с, длительность батча ~96 мс.

---

## 6. Вне объёма (не трогаем)
- Нулевые дропы и инвариант баланса — стабильны; схему каналов не менять без подтверждённого выигрыша.
- Проблема 1 (выбросы длительности батча до 4,234 мс) — отдельный план по записи, здесь не рассматривается.