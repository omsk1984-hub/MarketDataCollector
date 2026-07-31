# План: локализация и снижение lock contention (1,459 за прогон)

**Источник:** [`counters-analysis_20260731_093439.md`](plans/counters-analysis_20260731_093439.md:164) — Проблема 1: `process_runtime_dotnet_monitor_lock_contention_count_total` вырос с 241 до **1,459** за прогон 1.7M тиков (~88с), при стабильном пуле 9–12 потоков и чистой очереди.

**Контекст:** прогон 093439 — лучший за серию (дропы 0.31%, аллокации ~900 Б/тик, thread pool активность ↓17×). Снижение contention — **не срочная оптимизация**, направлена на уменьшение расходов CPU, не влияет на корректность/дропы.

---

## 1. Что уже установлено по коду

### 1.1 Явных `lock`/`Monitor` в hot path нет
Регекс-поиск по `src/MarketDataCollector.Application` не нашёл ни одного `lock (`, `Monitor.` или `SemaphoreSlim` на пути обработки. Единственные синхронизационные операции — `Interlocked.*` (lock-free):
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:165) — `Interlocked.Increment(_totalIncomingCount)` на каждый тик;
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:879) — `Interlocked.Add` для `_totalReceivedCount` / `_processedCount` на каждый батч;
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:821) — сэмплинг trace через `Interlocked.Increment(_processBatchCounter)`;
- [`SlidingWindowCounter`](src/MarketDataCollector.Core/Utilities/SlidingWindowCounter.cs:48) — `Interlocked.*` для RPS, batch-инкремент.

**Вывод:** метрика `.NET Monitor` фиксирует контеншен на **внутренних блокировках фреймворка**, а не на собственных примитивах кода.

### 1.2 Кандидаты внутреннего контеншена
Конфигурация ([`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:22)): `UseSingleConsumer=true`, `ChannelCapacity=150000`, `BatchChannelCapacity=40`, `MinBatchSize=2500`.

1. **Входной канал `Channel<TickData>`** ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:231)):
   - создан как `SingleReader=true, SingleWriter=false` → `TryWrite` из **3 потоков-продюсеров** (btcusdt / ethusdt / solusdt, [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:61));
   - при 19K тиков/сек и бэклоге 1.5K–5K внутренний spin/lock канала конкурирует. Это **главный кандидат** на уровне записи в очередь.

2. **OTel-счётчики (`Counter<long>.Add`)**: `TicksIncoming.Add` вызывается на каждый тик из 3 потоков ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:167)). Агрегация метрик OpenTelemetry использует внутренние `lock`/`Monitor` (AggregatorStore) — при 19K инкрементов/сек это даёт заметный контеншен. **Главный кандидат** на уровне телеметрии.

3. **DI `CreateScope()` на каждый батч** ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:862)) — создание scope (~660 батчей/прогон, 2500 тиков) → незначительно, но не исключаем.

### 1.3 Текущий сбор НЕ ловит contention
[`common-functions.ps1`](scripts/common-functions.ps1:200) запускает `dotnet-trace collect --profile gc-verbose`, который собирает только аллокации/GC и **не включает `ContentionKeyword`**. Поэтому в `allocation_trace_20260731_093439.nettrace` данных о contention нет — нужен отдельный прогон.

---

## 2. Шаги локализации (диагностика)

### Шаг A — собрать contention trace
Запустить прогон 1.7M тиков с трассировкой контеншена (профиль `cpu-sampling` + `ContentionKeyword`, или провайдер `Microsoft-DotNETCore-SampleProfiler` / `-e` событий `Microsoft-Windows-DotNETRuntime` c keyword). Цель — получить стек-трейсы, где поток ждёт монитор.

Команда (для документирования в плане):
```
dotnet-trace collect --process-id <pid> \
  --providers Microsoft-Windows-DotNETRuntime:0x40000004001C:5 \
  --output traces/contention_<ts>.nettrace --duration 90
```
(0x40000004001C включает `ContentionKeyword=0x4000` на уровне verbose `5`; фактический mask уточнить под версию runtime.)

Альтернатива без trace — временно включить в `appsettings.json` `ProcessBatchTraceSampling=1` и штатный OTLP-трейсинг, но для точной локализации по стекам предпочтителен dotnet-trace.

### Шаг B — измерить вклад по стекам
Разобрать speedscope-конверт trace: группировать события `Contention` по фреймам.
Ожидаемые кластеры:
- `System.Threading.Channels.Channel...TryWrite/WriteCore` — канал;
- `OpenTelemetry` / `AggregatorStore` / `Metric` — OTel-счётчики;
- `Microsoft.Extensions.DependencyInjection...CreateScope` — DI.

### Шаг C — определить преобладающий источник
Сравнить суммарное время ожидания и число contention-событий на каждый кластер. Дальнейшая оптимизация выбирается по фактическому результату, не по догадке.

---

## 3. Варианты оптимизации (после локализации)

### Вариант 1 — если доминирует OTel-счётчики (наиболее вероятно)
- **Агрегировать `TicksIncoming` локально:** инкрементить `Interlocked`-счётчик на тик, а `.Add` на счётчик-телеметрию делать пачками (раз в N тиков или раз в 100–2500) либо вынести за hot path.
- **Снизить частоту тега-тегирования:** уже кэшированы теги (`ExchangeTagBinance` и т.д.), но сам вызов `Add` остаётся в hot path.
- Цель — сократить число `lock`-секций в секунду с ~19K до ~7–75 (по числу батчей).

### Вариант 2 — если доминирует входной канал
- **Разделить продюсеров по отдельным каналам** (per-symbol channel) с маршрутизацией в Collector — убрать конкуренцию трёх продюсеров за один `TryWrite`. Требует переработки `ProcessTickAsync` + Collector.
- Либо перейти на `SingleWriter=false` уже так, а при разделении каналов каждый станет `SingleWriter=true` — меньше внутренних блокировок.

### Вариант 3 — если доминирует DI
- Переиспользовать один `IServiceScope`/репозиторий в рамках Writer-цикла вместо `CreateScope()` на каждый батч (осторожно: thread-safety репозитория/контекста).

### Правило принятия
Применять **только один целевой вариант**, который подтверждён шагом C. Не оптимизировать по предположению.

---

## 4. Критерии завершения
- Повторный прогон 1.7M (режим all, 90с) при тех же настройках нагрузки.
- `lock_contention` заметно ниже 1,459 (цель — кратно ниже), без роста дропов выше 0.31%, без роста аллокаций и времени батчей.
- Отсутствие регрессий по записи (97%) и дренажу канала (100%).

---

## 5. Отказ от дальнейших действий (не входит в план)
- Проблема 2 (51,234 дубликатов/дропов) и Проблема 3 (пик бэклога на остановке входа) из анализа **не трогаем** — они не критичны и в плане по contention не участвуют.
- Существующие бинарные `nettrace`/`gcdump` прогона 093439 не содержат contention-событий (профиль `gc-verbose`) — использовать их нецелесообразно; нужен новый contention-прогон.
