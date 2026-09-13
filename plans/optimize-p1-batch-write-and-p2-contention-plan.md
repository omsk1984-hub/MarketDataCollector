# План устранения P1 (медленная запись батчей) и P2 (contention/CTC-суспензии)

**Дата:** 13.09.2026 · Основание: `plans/counters-analysis_20260913_033417.md` (пункты 1 и 2), диагностика `plans/diagnose-slow-batch-write-A0-result.md`.

---

## Контекст (что показал анализ прогона 033417)

| Пункт | Проблема | Диагноз |
|---|---|---|
| **P1** | Batch duration avg **949 мс** / p50 662 мс / max **3.13 с** при `WriteDurationWarningMs=200` — устойчиво, 5-й прогон подряд | A0-диагностика: серверный INSERT быстр (~48 мс на 2500 уникальных). Корень был в **timestamp-эпохе FakeTickServer** → вся нагрузка падала в одну растущую `rawticks_default`, дорожа уникальный индекс |
| **P2** | TopN `gc-verbose`: `CancellationTokenSource.Register/CancelAfter/CreateLinkedTokenSource` ≈ **38%** CPU; contention +2125 | Может быть как real hot path, так и артефакт профилировщика `gc-verbose` (JFR/GC сам активно использует CTC) |

### Важное обновление (проверено по коду)
- ✅ **FakeTickServer уже исправлен**: [`TickGeneratorService.cs`](../tests/FakeTickServer/TickGeneratorService.cs:122) — `_syntheticTimeMs = DateTime.UtcNow` (реальная дата), данные лягут в day-партиции, `rawticks_default` расти не будет. **A0-[P0] выполнен.**
- В hot path (канал не пуст) `linkedCts` **не создаётся** — он нужен только в flush-ветке при пустом канале (`MarketDataProcessor.cs:443`, `:682`). При активной нагрузке канал не пуст, т.е. **большая часть CTC-времени из topN `gc-verbose` — артефакт инструментации**, а не прод-код.

---

## Часть 1 — P1: ускорение записи батчей

### A1. Увеличить MaxBatchSize 2500 → 5000 (в LoadTest и Production)
**Обоснование (из A0):** сервер выполняет INSERT 2500 с ~48 мс. Оверхед клиент↔сервер (round-trip, кодирование Npgsql `numeric[]`) фиксирован на батч, независимо от размера. Увеличение вдвое **удваивает** пропускную (тиков/единицу времени) при **том же** round-trip оверхеде. Middle batch пусть 2500 тиков, коммит реже.
- `appsettings.LoadTest.json`: `MinBatchSize`/`MaxBatchSize` 2500 → **5000** (не трогая `MinPartialBatchSize=1000`).
- `appsettings.Production.json`, `appsettings.json`: то же.

### A2. Добавить логирование «тихих хвостов» в RawTickRepository
Сейчас лог `BulkCopy (TickData) phase-timing` пишется при `totalMs > 150`. Добавить пометку **«tail»** для батчей > 1000 мс и расширить счётчики: `inserted_count` — чтобы видеть, были ли дубли. Это операционная видимость хвостов (рекомендация A0-[P2]).
- `RawTickRepository.BulkInsertFastAsync(TickData)`: в `if (totalMs > 150.0)` добавить суффикс `tail={Tail:B}` и `inserted={Inserted}`.

> ⚠️ **Важно:** одно только A1+A2 не «докажет» устранение — нужен контрольный прогон. Основной эффект ожидается от **уже исправленного генератора** (данные в day-партициях). A1 — доп. выигрыш.

## Часть 2 — P2: contention/CTC-суспензии

### B1. Точно локализовать источник (не менять прод-код вслепую)
- TopN `gc-verbose` для CTC ненадёжен (артефакт инструментации). Прогнать `start_loadtest.ps1 -TraceProfile contention-cpu -MaxTicks 1700000 -Rps 25000` и сравнить топ CTC в **CPU-профиле** (не gc).
- Проверить: в горячем цикле (канал не пуст) CTC не задействован → при подтверждении артефакта **изменения прод-кода по P2 не требуются**, только зафиксировать в отчёте.

### B2. (если B1 покажет real hot path) Сократить создание CTC в flush-ветке
Если contention подтвердится в прод-коде, оптимизировать flush-путь `CollectorLoopAsync`/`ProcessBatchesAsync`:
- Заменить `CancellationTokenSource.CreateLinkedTokenSource(...)` + `CancelAfter(...)` + `WaitToReadAsync(linkedToken)` на единый `WaitToReadAsync` с низким таймаутом через `Task.WhenAny` **только когда это даёт выигрыш** (см. гистограмму конкуренции). Однако текущая реализация с `CancelAfter` уже выигрышнее `Task.WhenAny` (по комментарию в коде, −23% CPU). Поэтому **менять не рекомендуется без подтверждённого горячего пути**.

---

## Принятие решения и риски

| Решение | Действие | Риск |
|---|---|---|
| A1 (batch 5000) | Меняем конфиги | Низкий. Частичный батч при flush всё равно сбрасывается по `MinPartialBatchSize`. Очередь батчей станет в 2 раза вместительнее на те же тики |
| A2 (лог хвостов) | Меняем RawTickRepository | Нулевой (лог добавочный) |
| B1 (локализация) | Только прогон | Нулевой (сбор данных) |
| B2 (оптимизация CTC) | **Отложено** до результата B1 | — |

**План остановится после B1** и потребует решения: если B1 подтвердит артефакт — P2 закрывается без изменений кода; если real hot path — B2 выполняется отдельным шагом.

---

## Список изменяемых файлов

1. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.LoadTest.json` — batch 5000
2. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json` — batch 5000
3. `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json` — batch 5000
4. `src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs` — лог хвостов
5. (опционально, после B1) `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs` — оптимизация CTC

## Критерий готовности
- `dotnet build` без ошибок.
- Существующие `dotnet test` зелёные (log-изменения не влияют на логику, конфиги — on test не задействованы).
- Контрольный прогон `start_loadtest.ps1`: batch duration avg < 400 мс, max < 1.5 с, хвосты >1 с отсутствуют; contention в `contention-cpu` не в топ-3.