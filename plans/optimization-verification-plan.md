# План верификации: проверка источников аллокаций, Gen2 и LOH

**Основание:** [`counters-analysis_20260804_184508.md`](plans/counters-analysis_20260804_184508.md:85-93) — ~764 байт/тик, 3 Gen2 за 90 с, LOH 26.6 MB.

**Текущее состояние кода:** большая часть hot-path оптимизаций уже реализована:
- `Utf8JsonReader` + интернирование ticker'ов — zero-alloc парсинг ✅
- `ArrayBufferWriter<byte>` вместо `MemoryStream` ✅
- `CounterBatcher` — zero-lock/zero-alloc телеметрия ✅
- `ArrayPool<TickData>` в Collector ✅
- `DedupKey` с кэшированным хэшем, `TryAdd` за один lookup ✅
- `ReusableArrayCache<T>` + pre-allocated `NpgsqlParameter[]` ✅
- `TickData` как `readonly record struct` ✅

**Цель плана:** эмпирически выяснить, какие именно объекты/паттерны дают оставшиеся ~764 байт/тик, Gen2-сборки и LOH.

---

## Шаг 1. Разбор `allocation_trace_20260804_184508.nettrace` — топ аллокаций

**Файл:** [`traces/allocation_trace_20260804_184508.nettrace`](traces/allocation_trace_20260804_184508.nettrace) (18.76 MB)  
**SpeedScope:** [`traces/allocation_trace_20260804_184508.speedscope.json`](traces/allocation_trace_20260804_184508.speedscope.json) (9.12 MB)

**Действия:**
1. Запустить `dotnet-trace report <nettrace> topN --inclusive -n 40` для топ-функций по inclusive CPU.
2. Запустить `dotnet-trace report <nettrace> topN -n 30` для топ-функций по exclusive CPU (leaf-функции).
3. Открыть `.speedscope.json` в SpeedScope, переключить на **"Left Heavy"** — найти аллокационные стеки.
4. Собрать воедино:
   - Какие типы аллоцируются больше всего? (String, byte[], object[], иное)
   - Какой % аллокаций приходится на Npgsql-сериализацию? (`NpgsqlParameter`, `byte[]` для `decimal[]`)
   - Какой % — на DI-контейнер? (`IServiceScope`, `ServiceProvider`)
   - Какой % — на `Channel<TickData>` (внутренние очереди Channel)?
   - Сколько аллокаций от `Activity`/трейсинга?

**Ожидаемый результат:** таблица топ-10 аллокаторов с % от общего объёма.

---

## Шаг 2. Сравнительный анализ gcdump: peak vs drained

**Файлы:**
- [`traces/snapshot_peak_20260804_184508.gcdump`](traces/snapshot_peak_20260804_184508.gcdump) (2.69 MB)
- [`traces/snapshot_drained_20260804_184508.gcdump`](traces/snapshot_drained_20260804_184508.gcdump) (2.59 MB)

**Действия:**
1. Запустить `dotnet-gcdump report <peak> -t 30` — топ-20 типов по общему размеру.
2. Запустить `dotnet-gcdump report <drained> -t 30`.
3. Сравнить **survivor ratio** по типам:
   - `System.Byte[]` — сколько байт остаётся в peak vs drained; какие размеры (>85 KB → LOH)
   - `System.String` — объём строковых данных
   - `TickData[]` — остатки после дренажа
   - `Npgsql.NpgsqlParameter[]` и связанные объекты
4. Определить **LOH-фрагментацию**: `(peak_bytes_total - survived_bytes_total)` — разница между occupied и живыми объектами.

**Ключевой вопрос:** растёт ли LOH за счёт Npgsql-сериализации (`decimal[]` → `byte[]` при передачи через UNNEST) или за счёт других источников (например, OTel-экспорт, DI-scope)?

---

## Шаг 3. Измерение аллокаций per-batch (ручная оценка)

После Шагов 1–2 должно быть понятно, какие компоненты аллоцируют больше всего. Если источники распределены равномерно, потребуется ручная оценка пер-батч расходов.

**Метод:** инструментирование кода через `GC.GetAllocatedBytesForCurrentThread()` до/после ключевых участков:

| Участок | Файл | Влияние |
|---|---|---|
| `ProcessBatchAsync` — от входа до вызова `BulkInsertFastAsync` | [`MarketDataProcessor.cs:846`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:846) | аллокации дедупликации + метрик |
| `BulkInsertFastAsync` — вся функция | [`RawTickRepository.cs:446`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) | аллокации Npgsql-параметров + сериализации |
| `ProcessTickAsync` — вызов | [`MarketDataProcessor.cs:167`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:167) | аллокация `TickData` + `TryWrite` |

> ⚠️ Инструментирование — временная мера для диагностики. После замера код возвращается к исходному состоянию.

---

## Шаг 4. Анализ lock contention

**Проблема:** 1,305 событий lock contention за прогон.

**Гипотезы из предшествующих планов:**
- **Входной канал `Channel<TickData>`** — `SingleWriter=false`, 3 WS-продюсера пишут в один канал ([`MarketDataProcessor.cs:189`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:189)). Внутренняя синхронизация канала — главный кандидат.
- **OTel-экспорт** — `OtlpLogExporter` + `RedirectHandler` ~17% CPU, внутренние lock.
- **`_scopeFactory.CreateScope()`** — на каждый батч (~680 раз).

**Действия:**
1. Собрать contention-трассу **с cpu-sampling одновременно** (предыдущая содержала только `--clrevents contention`, что дало пустой `topN`). Команда:
   ```
   dotnet-trace collect -p <PID> --providers Microsoft-DotNETRuntime:0x4001:5
   ```
   (ContentionKeyword=0x4000 + GCKeyword=0x0001)
2. После сбора — `dotnet-trace report <trace> topN -n 30` должен показать contention-стеки.
3. Если contention подтвердится на канале — оценить эффект от **per-symbol каналов** (каждый `SingleWriter=true`).

---

## Шаг 5. Целевые эксперименты (по результатам Шагов 1–4)

В зависимости от выявленных источников, применить **один вариант за раз** с повторным замером:

### Вариант A. Устранить Npgsql `decimal[]` → `byte[]` LOH-аллокации
Если Шаг 1/2 покажет, что Npgsql-сериализация `decimal[]` (price/volume) создаёт LOH-буферы:
- Заменить `decimal[]` на `text[]` с pre-formatted строками (как сделано в [`RawTickRepository.cs:339-340`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:339) для legacy-перегрузки) — Npgsql сериализует text[] компактнее.
- **Альтернатива:** перейти на `numeric[]` (Npgsql native) с кастомным конвертером, сокращающим размер LOH-буфера.
- **Оценка эффекта:** если Npgsql аллоцирует по ~20 KB LOH на батч × 680 батчей = ~13.6 MB. Сокращение на 50% даст ~−7 MB LOH.

### Вариант B. Кэшировать `IServiceScope` в WriterLoop
Если Шаг 3 покажет, что `CreateScope()` даёт заметные аллокации:
- Создать один scope в [`WriterLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:586) и переиспользовать его для всех батчей:
  ```csharp
  using var scope = _scopeFactory.CreateScope();
  var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();
  ```
- **Риск:** `RawTickRepository` — Scoped (EF Core DbContext), переиспользование может привести к росту контекста (change tracker). Требует проверки: `AsNoTracking` или `UseQueryTrackingBehavior(NoTracking)`.
- **Оценка эффекта:** одна аллокация scope ~1.5–2 KB × 680 = ~1–1.3 MB за прогон.

### Вариант C. Per-symbol каналы (снижение contention)
Если Шаг 4 подтвердит contention на входном канале:
- Разделить `channels[0]` на 3 канала по числу символов (btcusdt, ethusdt, solusdt), каждый с `SingleWriter=true`.
- Collector читает из всех трёх через `Channel<TickData>.Reader.WaitToReadAsync` в цикле или `Task.WhenAny`.
- **Оценка эффекта:** contention должен снизиться кратно (c 3 потоков на один lock до 3 отдельных lock'ов).

### Вариант D. Сократить OTel-экспорт
- Увеличить `MetricFlushBatchInterval` (с 1 до 5–10) — уменьшить число вызовов `Counter<T>.Add`.
- Отключить `OtlpLogExporter` в load test (если не нужен).
- **Оценка эффекта:** снижение CPU на OTel-экспорте ~17% → ~8–10%, contention снизится пропорционально.

---

## Шаг 6. Контрольный замер

После каждого эксперимента:
1. Запустить `.\run_all_profiler.ps1` с теми же параметрами (1.7M тиков, 90 с).
2. Снять `counters_*.csv` → сравнить:
   - `байт/тик` (цель: < 700)
   - `Gen2/90с` (цель: ≤ 2)
   - `LOH size` (цель: < 20 MB)
   - `lock contention` (цель: < 500)
3. Убедиться в сохранении:
   - 0 дропов
   - backlog ≤ 5
   - эффективность записи ~97%
   - инвариант баланса

---

## Порядок выполнения

| # | Задача | Инструмент | Зависимость |
|---|---|---|---|
| 1 | Разбор `allocation_trace` в SpeedScope + topN | `dotnet-trace report`, speedscope.app | trace на диске |
| 2 | Сравнение `snapshot_peak` vs `snapshot_drained` | `dotnet-gcdump report` | gcdump на диске |
| 3 | Инструментирование per-batch аллокаций (при необходимости) | `GC.GetAllocatedBytesForCurrentThread()` | Шаг 1–2 не дал однозначного ответа |
| 4 | Сбор contention-trace + cpu-sampling | `dotnet-trace collect --providers ...` | — |
| 5 | Эксперимент A (Npgsql LOH) или B (Scope) или C (каналы) | Code mode | Результаты Шага 1–3 |
| 6 | Контрольный замер | `run_all_profiler.ps1` | После каждого эксперимента |

## Ключевые файлы для изменений

- [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) — `BulkInsertFastAsync` (тип массивов prices/volumes)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:586) — `WriterLoopAsync` (scope lifetime)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:237) — каналы (per-symbol routing)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:909) — OTel BatchWriteDuration KVP per batch
- [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:281) — `FlushMetricBatchers` и метрики

## Критерии завершения

- Понятно, какие типы/компоненты дают > 80% оставшихся аллокаций (подтверждено trace или gcdump).
- Для каждого источника сформирована конкретная рекомендация с ожидаемым эффектом.
- Выполнен хотя бы один целевой эксперимент с контрольным замером до/после.