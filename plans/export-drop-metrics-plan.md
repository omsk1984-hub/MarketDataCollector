# План: вернуть/экспортировать счётчики дропов `ticks_dropped` / `ticks_dropped_silently` в /metrics и CSV

## 1. Проблема

В [отчёте `counters-analysis_20260804_143625.md`](plans/counters-analysis_20260804_143625.md:133) зафиксировано:

> ⚠️ Отсутствуют метрики `ticks_dropped_count_total` / `ticks_dropped_silently_count_total` в CSV. Дропы подтверждаются только лог-счётчиком `MarketDataProcessor`: `реально дропнуто: 0`.

### Корневая причина

Обе метрики объявлены как cumulative `Counter`:

- [`TicksDropped`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:102) — инкремент только через [`IncrementTicksDropped`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:266), вызывается в [`MarketDataProcessor.cs:192`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:192) только при `TryWrite=false`. При штатной нагрузке дропов нет → `Add` не вызывается → сэмпл не создаётся.
- [`TicksDroppedSilently`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:178) — `Add(droppedDelta)` в [`Worker.cs:239`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:239) выполняется только при `droppedDelta > 0`. При нулевых дропах → сэмпл отсутствует.

Prometheus-экспортер OpenTelemetry создаёт сэмпл для `Counter` только после первого `Add` (появления агрегата). Поскольку за прогон значение всегда 0, метрики не попадают ни в `/metrics`, ни в CSV, собираемый [`CountersCollector`](tools/Profiler/Services/CountersCollector.cs:12).

## 2. Цель

Обеспечить видимость счётчиков дропов **всегда**, в том числе при нулевом значении, чтобы штатный мониторинг (Prometheus + counters CSV) мог:
1. подтверждать «дропов нет» по самим метрикам, а не по логу;
2. ловить начало роста дропов до достижения лог-порогов.

## 3. Предварительный спринт-верификации (выполняется ДО выбора варианта)

Пользователь зафиксировал порядок: **сначала эмпирически подтвердить, что ObservableGauge с нулевым значением экспортируется в `/metrics` и CSV**, и только потом фиксировать вариант.

Способ без изменения кода: в проекте уже есть `ObservableGauge` [`ChannelFillLevel`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:167), который при старте воркера без нагрузки имеет значение 0 по каждому каналу.

Шаги спринт-проверки:

1. Запустить воркер без нагрузки (FakeTickServer выключен).
2. Снять `/metrics` сразу после старта.
3. Убедиться, что `processor_channel_fill_level{channel_index="0"} 0` присутствует в выводе.
4. Прогнать короткий сбор counters CSV (или вручную снять один сэмпл) и убедиться, что `processor_channel_fill_level` с нулём попал в CSV.

Итог проверки → в раздел 3: если ObservableGauge экспортирует ноль — фиксируется **Вариант A (полная замена)**, иначе — проработка альтернатив (например, гарантированная инициализация или принудительный flush на старте).

## 4. Рассмотренные варианты

### Вариант A (рекомендуется): Observable-метрики вместо Counter в hot-path

Перевести указание «текущего накопленного числа дропов» на ObservableGauge (`observeValues`), которые экспортируются при **каждом** сборе, даже при значении 0 (по аналогии с уже работающим [`ChannelFillLevel`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:167)).

**Плюсы:**
- Сэмпл всегда присутствует в `/metrics` и CSV (включая 0).
- Источник значения — существующий точный счётчик [`GetEstimatedDroppedCount()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:1028), не требует накопления через бэтчеры.
- Нет влияния на hot-path (чтение из накопленного `long`).

**Минусы:**
- Тип изменится с `counter` на `gauge`: в PromQL для него перестанет работать `rate()`/`increase()`. Для мониторинга дропов это допустимо, т.к. важнее видимость абсолютного значения и его приращения.

### Вариант B: гарантированная инициализация `Counter` через `Add(0)`

Вызывать `Add(0)` при старте воркера, чтобы агрегат существовал.

**Минусы:** поведение зависит от версии OTel/Prometheus-экспортера (для `Counter` `Add(0)` может не создавать запись); не гарантирует видимость нуля. Ненадёжно — отклонено.

### Вариант C: отдельный ObservableGauge-дубль только для CSV

Добавить наблюдаемую метрику-дублёр рядом с Counter.

**Минусы:** дублирование, две метрики с разной семантикой; избыточно. Отклонено.

## 5. Реализация (Вариант A)

### 4.1. `ticks_dropped_silently` → ObservableGauge

В [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:178) заменить `Counter TicksDroppedSilently` на `ObservableGauge<long>` с `observeValues`, читающим значение из провайдера-делегата.

- Провайдер должен возвращать текущее `GetEstimatedDroppedCount()`. Так как `MarketDataTelemetry` — статический класс без ссылки на процессор, ввести статическое поле-источник `Func<long>`/`long`, которое выставляет [`Worker`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs) в цикле health-check (аналогично `SetChannelFillLevel`).
- В [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:236) убрать блок `TicksDroppedSilently.Add(droppedDelta)` и вместо этого обновлять источник значения (можно как побочный эффект в цикле).
- Имя метрики сохранить `ticks.dropped.silently`, чтобы не ломать существующий анализ/дашборды. В CSV оно появится как `ticks_dropped_silently` с типом `gauge`.

### 4.2. `ticks_dropped` → сохранить hot-path, добавить Observable-отображение

`TicksDropped` по-прежнему инкрементируется в hot-path через `CounterBatcher` ([`IncrementTicksDropped`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:266)) — это критичный путь, менять его нельзя.

Чтобы метрика была видна при нуле, добавить **накопительный источник** отдельно от OTel-инструмента:

- Ввести `long`-аккумуляторы (`Interlocked`) на 3 exchange-тега (`binance`/`kraken`/`unknown`) в `MarketDataTelemetry`, которые инкрементируются внутри `IncrementTicksDropped` (без аллокаций).
- Объявить `ObservableGauge<long> TicksDropped`, `observeValues` которого возвращает текущие суммы аккумуляторов по тегам exchange. Это гарантирует сэмпл даже при нуле.
- Либо, если расхождение между OTel-Counter и аккумулятором нежелательно, полностью заменить `Counter TicksDropped` + бэтчеры на Observable с аккумуляторами (анализ: в `FlushMetricBatchers` бэтчеры `TicksDropped*` убрать, аккумуляторы оставить для `IncrementTicksDropped`). Рекомендуется **замена целиком** — единый источник истины, без дублирования счёта.

### 4.3. Проверка экспорта при нуле

- Отдельно убедиться, что ObservableGauge с нулевым значением попадает в вывод `/metrics` (по аналогии с `ChannelFillLevel`). При необходимости — быстрый локальный запуск `/metrics` и `curl` без нагрузки.

### 4.4. Обновление анализа

- В скилле/инструменте анализа counters (если он ищет метрики по конкретным именам `*_total`) учесть, что `ticks_dropped`/`ticks_dropped_silently` теперь имеют тип `gauge` и имя без суффикса `_total`. Проверить [`.roo/skills/metrics-analysis`](.roo/skills/metrics-analysis/SKILL.md) на предмет жёсткой привязки к `_total`.

## 5.1. Результаты спринт-верификации и реализации (факт 2026-08-04)

### Спринт-верификация (раздел 3) — подтверждено

Запуск Worker без нагрузки (FakeTickServer выключен) и снятие `/metrics` на `http://localhost:5010/metrics`:

- `processor_channel_fill_level_count{channel_index="0"} 0` — **присутствует** в `/metrics` сразу после старта.
- В counters CSV (`CountersCollector` парсит тот же `/metrics`) метрика `processor_channel_fill_level_count` с типом `gauge` и значением `0` попадает с первого сэмпла (см. `traces/counters_20260804_143625.csv:685`).
- `ticks_dropped` / `ticks_dropped_silently` (бывшие `Counter`) при нуле — **отсутствуют** в `/metrics`. Корневая причина подтверждена: Prometheus-экспортер OTel создаёт сэмпл только после первого `Add`.

**Вывод:** ObservableGauge экспортирует ноль → **Вариант A зафиксирован**.

⚠️ **Важное уточнение про имя:** экспортёр добавляет суффикс `_count` к имени ObservableGauge. Объявленное `ticks.dropped` становится `ticks_dropped_count`, а `ticks.dropped.silently` → `ticks_dropped_silently_count` (тип `gauge`). Это учтено в обновлении скилла анализа.

### Реализация (Вариант A) — выполнена

- [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs): оба счётчика переведены с `Counter` на `ObservableGauge<long>`.
  - `TicksDropped` — накапливается в 3 Interlocked-аккумуляторах по exchange (`binance`/`kraken`/`unknown`), `observeValues` читает их при каждом сборе. Бэтчеры `TicksDropped*` и их `Flush` убраны.
  - `TicksDroppedSilently` — значение выставляет `Worker` через `SetTicksDroppedSilently`, `observeValues` читает его.
- [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:236): вместо `TicksDroppedSilently.Add(droppedDelta)` вызывается `SetTicksDroppedSilently(estimatedDropped)`.
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:192): вызов `IncrementTicksDropped` не менялся (сигнатура та же), инкремент теперь без аллокаций и lock (Interlocked).
- [`SKILL.md`](.roo/skills/metrics-analysis/SKILL.md:67): обновлены имена `ticks_dropped_count` / `ticks_dropped_silently_count` (gauge, без `_total`).

### Проверка экспорта при нуле — подтверждено

После изменений, запуск Worker без нагрузки: `ticks_dropped_count{exchange="binance|kraken|unknown"} 0` и `ticks_dropped_silently_count 0` **присутствуют** в `/metrics`.

### Нагрузочный прогон 1.7M (`run_loadtest.ps1`) — подтверждено

Артефакты `traces/*_20260804_160733.*`, counters `traces/counters_20260804_160733.csv`, отчёт `traces/profiling_report_20260804_160733.md` (без предупреждений).

- `ticks_dropped_count` видна с **первого сэмпла** (3 exchange-тега, все `0`).
- `ticks_dropped_silently_count` видна с **первого сэмпла** (`0`) и корректно отслеживает рост дропов: `0 → 139352 → 213156 → 275600 → 302065` (DropOldest) — метрика ловит начало роста дропов в реальном времени.
- Полный вход: `ticks_incoming_count_total = 1700000`.
- `dotnet format` применён, `dotnet build` — 0 ошибок / 0 предупреждений.

## 6. Критерии готовности (DoD)

1. После запуска без нагрузки в `/metrics` присутствуют `ticks_dropped` и `ticks_dropped_silently` со значением `0`.
2. В counters CSV эти метрики появляются с первого сэмпла (не только при появлении дропов).
3. Hot-path не изменился: `IncrementTicksDropped` по-прежнему без аллокаций и lock (Interlocked).
4. `MarketDataProcessor` не инжектирует новые зависимости; семантика `GetEstimatedDroppedCount()` не меняется.
5. Прогон 1.7M (как в отчёте) даёт: при дропах 0 метрики показывают 0, а не отсутствуют.
6. Повторный анализ counters подтверждает отсутствие предупреждения про «отсутствующие метрики дропов».

## 7. Файлы

- [`src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:102) — смена деклараций, аккумуляторы, `observeValues`.
- [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:236) — отказ от `TicksDroppedSilently.Add`, выставление источника значения.
- Проверка: [`ModulePlugin` скилл анализа](.roo/skills/metrics-analysis/SKILL.md), `tools/Profiler/README.md`.