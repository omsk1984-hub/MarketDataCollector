# План: снижение lock contention за счёт батчевого сбора метрик

**Источник:** [`reduce-lock-contention-plan.md`](plans/reduce-lock-contention-plan.md:65) — Вариант 1: если доминируют OTel-счётчики, агрегировать `TicksIncoming` локально и выносить `Counter.Add` из hot path.

**Цель:** убрать вызовы `Counter<long>.Add` из per-message hot path (внутренние `lock`/`Monitor` в `AggregatorStore` OpenTelemetry) без изменения имён, тегов и семантики публикуемых метрик. Сохранить текущие агрегаты в Prometheus/OTLP ровно в том же виде, но с меньшей частотой `Add`.

---

## 1. Текущее состояние (установлено)

### 1.1 Per-message метрики в hot path (главные кандидаты)
- [`MarketDataProcessor.ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:163):
  - строка 165 — `Interlocked.Increment(ref _totalIncomingCount)`;
  - строка 167 — `MarketDataTelemetry.TicksIncoming.Add(1, GetExchangeTag(exchange))` — **на каждый тик, из 3 потоков-продюсеров**;
  - строка 187 — `TicksDropped.Add(1, ...)` — только при дропе (не hot path, но для единообразия перевести).
- [`BaseWebSocketClient.OnMessageReceived`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:398):
  - строки 400–401 — локальные `_msgRpsCounter.Increment()` + `Interlocked.Increment(ref _totalMessagesCount)`;
  - строки 404–406 — `WsMessagesReceived.Add(1, exchange, symbol)` — **на каждое сообщение** (экспансия тегов по `exchange`+`symbol`).

### 1.2 Per-batch метрики — НЕ трогаем
`TicksReceived`, `TicksProcessed`, `BatchWriteDuration`, `ChannelFill*`, `BatchChannelFill`, `AdaptiveBatchSize`, `ExceptionsByType` — вызовы ~раз на батч/интервал, вклад в contention пренебрежим.

### 1.3 Уже существующие локальные счётчики (переиспользуем паттерн)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:90) — `_totalIncomingCount`, `_totalDroppedCount`, `_processedRpsCounter`;
- [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:400) — `_msgRpsCounter`, `_totalMessagesCount`;
- кэшированные теги `ExchangeTagBinance`/`ExchangeTagKraken`/`ExchangeUnknownTag` и `ChannelIndexTags` ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:60)).

### 1.4 Точки, где удобно делать Flush
- **Writer loop** ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:251)) — вызывается на каждый батч (~660 за прогон 1.7M), однопоточно;
- **Финальный flush при остановке** — чтобы не потерять остаток <1 батча.

---

## 2. Архитектурное решение: `CounterBatcher`

Новый класс в `MarketDataCollector.Core/Telemetry/`:

```
CounterBatcher(Instrument instrument)
  - thread-safe: использует Interlocked.Increment (без lock)
  - hot path:  Add() -> Interlocked.Increment(ref _count)
  - один раз за интервал/батч: Flush() -> instrument.Add(Interlocked.Exchange(ref _count, 0), tags)
```

**Ключевые требования:**
- **Ноль аллокаций в hot path** — без создания `KeyValuePair` на каждый инкремент.
- **Ноль `lock` в hot path** — только `Interlocked`.
- **Имена/теги/единицы метрик не меняются** — экраны Prometheus/OTLP остаются прежними, меняется только частота `Add`.
- Поддержка **фиксированного набора тегов** (per-exchange, per-exchange+symbol) через предсозданные словари аккумуляторов.

**Дизайн с учётом тегов:**
- Так как теги у `TicksIncoming` — только `exchange` (Binance/Kraken/unknown), а у `WsMessagesReceived` — `exchange`+`symbol`, удобно иметь **отдельный экземпляр `CounterBatcher` на каждую комбинацию тегов**.
- Для `TicksIncoming` — `Dictionary<string, CounterBatcher>` с ключом `exchange` (статические, малый размер), либо три явных поля `_incomingBinance/_incomingKraken/_incomingOther`.
- Для `WsMessagesReceived` — `ConcurrentDictionary<(string exchange, string symbol), CounterBatcher>` (создаётся лениво при первом сообщении, обычно 3 комбинации). Доступ в hot path — `TryGetValue`/индексация без аллокаций, но сами `KeyValuePair` тегов создаются только в `Flush`.

> **Производительность hot path:** `Dictionary.TryGetValue` для 3 фиксированных ключей почти free. Если требуется строго минимум — для `TicksIncoming` выбрать фиксированные поля (без словаря), т.к. число бирж известно из конфига.

### Компромисс по симметрии (правило принятия из исходного плана)
Применяем **один механизм** (батчевый сбор) к обоим per-message счётчикам, но **каждый батчер настраивается на свой фиксированный набор тегов**. Не вводим новые метрики и не меняем существующие.

---

## 3. Изменения по файлам

### 3.1 Новый файл `src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs`
- Конструктор принимает `Counter<long>` (или имя метрики + теги).
- Поля: `long _count; KeyValuePair<string, object?>[] _tags; Counter<long> _counter;`.
- `[MethodImpl(AggressiveInlining)] void Add()` — `Interlocked.Increment(ref _count);`.
- `void Flush()` — `long n = Interlocked.Exchange(ref _count, 0); if (n != 0) _counter.Add(n, _tags);` (перегрузка `Add(TagList)` или массивом — проверить сигнатуру; допускается `Span<KeyValuePair>`).
- `void FlushAndReset()` — для финального сброса.
- Регистрация в DI не нужна — создаётся и живёт в `MarketDataTelemetry` как статический реестр или в инстансах `MarketDataProcessor`/`BaseWebSocketClient`.

**Размещение реестра:** чтобы `Flush` был доступен и из `MarketDataProcessor` (writer loop), и из `BaseWebSocketClient`, целесообразно держать аккумуляторы как **статическое поле в `MarketDataTelemetry`** (например, `internal static ... Batchers`) с методами `FlushAll()`. Это централизует и упрощает финальный сброс.

### 3.2 `MarketDataProcessor.ProcessTickAsync` ([строка 163](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:163))
- Заменить `Interlocked.Increment(ref _totalIncomingCount)` + `TicksIncoming.Add(...)` (строки 165, 167) на:
  - `Interlocked.Increment(ref _totalIncomingCount);` (оставить для существующих геттеров);
  - `MarketDataTelemetry.TicksIncomingBatcher.Increment(exchange);` — инкремент без `Add`.
- Заменить `TicksDropped.Add(...)` (строка 187) на `TicksDroppedBatcher.Increment(exchange);` (опционально, шаг 5).

### 3.3 Writer loop + финальный flush ([`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:251))
- В начале каждой итерации writer loop (после формирования батча) — `MarketDataTelemetry.FlushMetricBatchers();`.
- При остановке/финале обработки — ещё один `FlushMetricBatchers()` (не терять остаток).

### 3.4 `BaseWebSocketClient.OnMessageReceived` ([строка 398](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:398))
- Заменить `WsMessagesReceived.Add(1, exchange, symbol)` (строки 404–406) на `MarketDataTelemetry.WsMessagesReceivedBatcher.Increment(exchange, symbol);`.
- `Flush` для этого батчера — тот же `MarketDataTelemetry.FlushMetricBatchers()`, вызываемый из writer loop (общий тик).

### 3.5 `MarketDataTelemetry.cs`
- Добавить статические батчеры-реестры и `internal static void FlushMetricBatchers()`.
- Сохранить публичные `Counter`-поля без изменений (на них ссылаются тесты и Prometheus-конфиг).

---

## 4. Конфигурируемая периодичность (опционально)
В `MarketDataProcessorOptions` добавить `MetricFlushBatchInterval` (по умолчанию — каждый батч) или интервал в мс. Если требуется меньше `Add`, можно флашить раз в N батчей. По умолчанию — флашить каждый батч (простота, предсказуемость, ~660 `Add`/прогон вместо 1.7M).

---

## 5. Критерии завершения (что проверить в Code-режиме)

### Функциональные
- Имена метрик, единицы, теги **не изменились**:
  - `ticks.incoming` по `exchange` — сумма всех батчеров равна прежнему суммарному счётчику;
  - `ws.messages.received` по `exchange`+`symbol` — аналогично;
  - `ticks.dropped` (если переведён).
- Сумма `Add` за прогон совпадает с числом тиков (нет потерь/двойного учёта) — проверить по итоговым счётчикам `_totalIncomingCount`.

### Unit-тест (шаг 6)
- После N `Increment()` без `Flush` счётчик равен 0 (данные в локальном буфере);
- после `Flush()` значение в `Counter` равно N;
- теги корректны;
- поведение `Flush` с нулевым остатком — no-op (не вызывает `Add(0)`).

### Производительность (шаг 7)
- Повторный прогон 1.7M (режим all, 90с) при тех же настройках нагрузки.
- `lock_contention_count_total` **кратно ниже 1,459**.
- Дропы не выше 0.31%, аллокации ~900 Б/тик, время батчей не выросло.
- Запись 97% и дренаж канала 100% без регрессий.

---

## 6. Вне области (не входит)
- Проблемы 2 и 3 исходного анализа (дубликаты/дропы, пик бэклога) — не трогаем.
- Оптимизация входного канала (Вариант 2) и DI-скоупов (Вариант 3) — применяются **только если** contention trace (шаг A/B исходного плана) покажет доминирование канала/DI. Наш план реализует Вариант 1 и подтверждается его собственным trace.
