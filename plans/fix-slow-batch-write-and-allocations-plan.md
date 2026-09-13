# План: устранение P1 (медленная запись батчей) и P2 (высокие аллокации)

**Дата:** 13.09.2026
**Основание:** [`plans/counters-analysis_20260913_021227.md`](counters-analysis_20260913_021227.md) (и идентичный прогон 20:52).
**Статус:** черновик, требует утверждения перед реализацией (см. `.roo/rules/ROO-rules.md`).

---

## Контекст: данные из прогонов

| Метрика | 20:52 | 02:13 | Значение |
|---|---|---|---|
| `ticks_batch_write_duration`, avg | 949 мс | **949 мс** | повторяемо |
| то же, p50 | 662 мс | 662 мс | повторяемо |
| то же, max | 3 131 мс | **3 131 мс** | повторяемо |
| `inserted/batch` | 96.9% | 96.9% | стабильно |
| Аллокации | +1 286 MB | **+1 281 MB** | ~18.6 MB/с |
| LOH heap (финал) | 17.89 MB | **17.64 MB** | стабильно |
| Lock contention | 2 295 | **2 298** | растёт с аллокациями |

**topN exclusive (02:13):** `CancellationTokenSource.Register` 15.9%, `WaitToReadAsync` 14.95%, `WebSocketConnectionManager.ReceiveAsync` 9.69%, `CancellationTokenSource.CancelAfter` 9.05%, `Byte[].Rent` 6.53%, `EventPipeMetadataGenerator` 6.43%.

**Диагноз:**
1. **P1 не связан с клиентским парсингом.** Запись идёт через единый `WriterLoopAsync` → `BulkInsertFastAsync` (UNNEST + `ON CONFLICT DO NOTHING`). Длительности **повторяются с точностью до единицы** между прогонами — это не случайный фон, а стабильное свойство пути записи.
2. **P2 сосредоточен в WS hot-path** (суспензии канселляций `CTC.Register/CancelAfter/CreateLinkedTokenSource`, `Byte[].Rent`) и в самом парсинге (`.`ctor Decimal из topN, `String`/`KeyValuePair`).

Ниже — план с доказательными шагами: сначала **измерение**, потом **точечные изменения**, затем **верификация** повторным прогоном.

---

## Часть A. P1 — медленная запись батчей (avg 949 мс, max 3.1 с)

### A.0 Диагностика перед изменениями (обязательный первый шаг)

1. **Профилирование БД отдельно от приложения.** Запустить тот же `BulkInsertFastAsync` SQL вручную (pgAdmin/psql) с массивом 2500 строк: измерить чистое время серверного `INSERT ... SELECT unnest(...) ON CONFLICT DO NOTHING`. Если серверный запрос быстрый (<50 мс) — узкое место в клиенте/Npgsql/сети; если медленный — в планировщике Postgres или индексах.
2. **Проверить батч-статистику по сэмплам** в `counters_*.csv`: насколько `sum/count` \
`ticks_batch_write_duration_milliseconds` равномерно растёт или есть «ступени» (признак блокировок в БД). Снять `max` по каждому из 22 сэмплов — где именно пики 3.1 с.
3. **Паузы GC:** сопоставить моменты медленных батчей с `gc_allocations_*`/`gc_duration_*` по тем же сэмплам — отделить стоимость самого INSERT от пауз GC, съедающих budget.
4. **Проверить `Explain` серверного плана** запроса UNNEST: правильные ли индексы (unique `(ticker, exchange, timestamp)`), нет ли seq scan, не `ON CONFLICT` спамит conflict при ~3% дублей.

> **Правило:** если после A.0 окажется, что узкое место — планировщик Postgres/индексы/страница, изменения A.1–A.4 в коде не дадут эффекта. Всегда начинать с A.0.

### A.1 Убрать серверный каст `unnest(@prices::text[])::numeric` (high-value)

В [`RawTickRepository.cs:393`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:393) `price`/`volume` передаются как `text[]` и кастуются на сервере в `numeric`. Это заставляет Postgres парсить 2500 строк в `numeric` на каждый батч — дорого и стабильно.

- **Цель:** передавать числовые поля бинарно как `numeric[]` (Npgsql умеет кодировать `Numeric[]`), либо использовать `COPY`-подобный путь. Убрать из SQL `::text[]::numeric`.
- При невозможности `Numeric[]` — перейти на `bigint`/`int` (масштаб фикс 1e-8 и т.п. из `DecimalHelper`) для hot-path, `numeric` оставить только на краю.
- **Ожидание:** минус значительная серверная цена парсинга на батч.

### A.2 Переиспользование `string[]` буферов вместо `ToString` на каждый тик

В [`RawTickRepository.cs:383-384`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:383) `price[i] = e.Price.ToString(...)` создаёт **2500 короткоживущих строк за батч** (и это попадает в LOH/GC). Это одновременно аллокационная нагрузка (связана с P2) и цена на каждый батч.

- Буферизация: так как формат Decimal предсказуем, писать цифры прямо в `char[]` без объекта `string` (нет аллокации) — см. аналогии `ws-hotpath-decimal-parser-plan.md`.
- Это снижает и p50, и аллокации параллельно.

### A.3 Параллельность записи батчей (Writer — единственное последовательное место)

Сейчас `WriterLoopAsync` пишет батчи **строго последовательно** (`await foreach` на одном writer-процессе). При 3.1 с на батч консьюмер просто ждёт.

- Аккуратно: **не** слепо распараллеливать на несколько подключений — уже упоминается `ON CONFLICT` и per-ticker routing. Нужен **одно подключение + конвейер** через `Npgsql` `BEGIN/COMMIT` в `COPY`-режиме, либо **2–3 писателя на дисажойнтные группы ticker'ов** (гарантируется отсутствие конфликтов за unique index — как в комментарии [`RawTickRepository.cs:355`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:355)).
- A.3 выполнять **только после** A.1/A.2 и повторного измерения: если сервер быстр, распаралл-ливание может не требоваться. Гипотеза требует проверки в A.0.

### A.4 (fallback) Настроить `WriteDurationWarningMs`-логирование

Добавить в `writer` предупреждение при `BatchWriteDuration > WriteDurationWarningMs`, чтобы медленные батчи были видны операционно (сейчас не логируются — P4 из отчёта).

---

## Часть B. P2 — высокие аллокации (~18.6 MB/с)

### B.0 Приоритизация по topN (данные 02:13)

Главные мишени exclusive:
- `CancellationTokenSource.Register` 15.9% + `CancelAfter` 9.05% + `CreateLinkedTokenSource` 4.98% ≈ **~30%** — аллокации суспензий отмены/таймаутов.
- `WaitToReadAsync` 14.95% + `WebSocketConnectionManager.ReceiveAsync` 9.69% + `ManagedWebSocket.ReceiveAsync` 5.71% — сам приём/чтение.
- `Byte[].Rent` 6.53% — аренда буфера приёма (уже через `ArrayPool<byte>`, см. ниже).
- `.ctor Decimal` 3.33% — парсинг Decimal в hot-path.

### B.1 Убрать суспензии `CancellationTokenSource.CancelAfter` из `WaitToReadAsync` fast-path (приоритет)

В `CollectorLoopAsync` и `ProcessBatchesAsync` уже есть fast-path «если канал содержит данные — сразу читаем без linked CTS» (комментарии [MarketDataProcessor.cs:436](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:436), [:675](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:675)). Продолжить линию:

- **Добавить fast-path на `channel.Reader.Count > 0` ПЕРЕД созданием `linkedCts`** (сейчас ветвление есть, но проверка `batchCount > 0` ограничивает). При активной нагрузке (`Count > 0`) таймерный флаш не нужен совсем — он нужен только при тишине канала. Это уберёт `CreateLinkedTokenSource`/`CancelAfter` (~30% topN) из самого частого сценария.
- Аналогично в WS-цикле: если есть «непрочитанное» в буфере — не заводить суспензию отмены.

### B.2 `Byte[].Rent` — переиспользование буфера приёма сохранить, но вернуть повторно

[`WebSocketMessageReceiver.cs:80`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:80): `tempBuffer = ArrayPool<byte>.Shared.Rent(...)` берётся один раз на loop — хорошо. Проверить, что **на каждый вызов `ReceiveAsync` не рентится новый буфер** внутри `ManagedWebSocket`/`WebSocketConnectionManager` ([ReceiveAsync:107](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:107)). topN `Byte[].Rent` 6.53% — вероятно именно там. Если да:
- Переиспользовать один `ArraySegment<byte>` на весь цикл (как `tempBuffer`), не аллоцировать сегмент на тик.
- **Цель:** довести `Byte[].Rent` в hot-path до ~0.

### B.3 Парсинг Decimal без аллокации строк/объектов

`.ctor Decimal 3.33%` + `ToString` в репозитории. Применить подход `ws-hotpath-decimal-parser-plan.md`: парсер в `char[]`/`Span<byte>` без создания `Decimal`/`string` в конвейере; финальное `Decimal`/`numeric` создавать только на границе записи. Это сократит dotnet_alloc в куче и LOH.

### B.4 Уменьшить `String`/`KeyValuePair` в парсере кадра WS

Если сообщение распознаётся как binance-тик — не строить промежуточные `KeyValuePair<String,Object>`/`Map` строки где это можно заменить tuple/структурой. Оценить по `nettrace` (см. часть C) — какие именно типы доминируют.

---

## Часть C. Бинарная диагностика (дополняет counters)

**Когда:** после A.0, если counters/эксплейс не дали однозначного ответа, или перед B.x для точного списка горячих типов.

```bash
# конвертация trace (если ещё нет .etlx) — профиль аллокаций
dotnet-trace report traces/allocation_trace_20260913_021227.nettrace analyze -profile-type GCAllocationTick
dotnet-gcdump report traces/snapshot_peak_20260913_021227.gcdump
dotnet-gcdump report traces/snapshot_drained_20260913_021227.gcdump
```

- По gcdump peak vs drained: сравнить Top types by count/size, Gen2/LOH fragmentation, survivor ratio — уточнить **что** занимает LOH 17.6 MB (ожидание: `string[]`/`Numeric`-буферы/структуры батчей).
- По nettrace: подтвердить долю `string`/`Decimal`/`KeyValuePair` в hot-path — для точного выбора B.3 vs B.4.

> Не выполнять, если A.0 + counters уже дают ответ.

---

## Часть D. Верификация

1. **Повторный прогон loadtest** тем же способом (1.7M тиков, 3 символа, batch 2500).
2. **Проверить в отчёте:**
   - `ticks_batch_write_duration_milliseconds`: avg/p50/max — ожидание снижения min в 2–4 раза, максимум «не 3.1 с».
   - `process_runtime_dotnet_gc_allocations_size_bytes_total`: ожидание < 1 GB за пик (цель ~ −30%+).
   - `gc_heap_size_bytes` loh / `gc_heap_fragmentation_size_bytes` loh: снижение LOH.
   - `monitor_lock_contention_count_total`: не растёт.
   - Инвариант дедупликации: по-прежнему **точный**, дропы **0**.
3. **Контроль регрессий:** `worker_out.log` без новых ошибок; `processed` около 96–97%; `received == incoming` (100%).

---

## Порядок внедрения (по шагам, после утверждения)

| Этап | Что | Модуль | Ожидаемый эффект |
|---|---|---|---|
| 0 | A.0 (профиль БД) + C (gcdump/nettrace) | — | фактическая причина P1/P2 |
| 1 | A.1 серверный каст `numeric` | `RawTickRepository` | ↓ серверная цена INSERT |
| 2 | A.2 переиспользование буферов `ToString` | `RawTickRepository` | ↓ аллокации + ↓ p50 |
| 3 | B.1 fast-path без `CancelAfter` при активном канале | `MarketDataProcessor` | −~30% topN (CTC) |
| 4 | B.2 переиспользование буфера приёма | `WebSocketMessageReceiver`/`WebSocketConnectionManager` | −`Byte[].Rent` |
| 5 | B.3 Decimal-парсер без аллокаций | hot-path WS + repo | ↓ Decimal/String/LOH |
| 6 | A.4 логирование медленных батчей | `MarketDataProcessor` | операционная видимость |
| 7 | **D. верификация** повторным прогоном | — | подтверждение эффекта |
| 8 | (условно) A.3 параллельные писатели | `MarketDataProcessor` + repo | если 1 серверный путь всё ещё медленный |

> **Не выполнять A.3 без повторного измерения после A.1/A.2** — до этого параллельность может быть лишней сложностью и риском.

## Критерии завершения

- `ticks_batch_write_duration` avg < 400 мс, max < 1 с (порог по-прежнему 200 мс на предупреждение).
- Аллокации < 1.3 GB за пик, LOH < 10 MB.
- Инвариант дедупликации точный, дропы 0.
- Нет регрессий пропускной способности (вход 100%, запись ≥ 96%).

## Открытые вопросы (уточнить перед реализацией)

1. Целевой лимит p50/avg длительности записи (я взял 400 мс avg — подтвердить).
2. Допустимо ли менять тип хранения `price/volume` на `bigint` (влияет на БД/синхронизацию с другими подсистемами), или нужен путь `Numeric[]` бинарно.
3. Сколько писателей допустимо в проде (если дойдём до A.3) — ресурсы/соединения к БД.