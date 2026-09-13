# A0-план: диагностика узкого места записи батчей (P1)

**Дата:** 13.09.2026
**Основание:** [`fix-slow-batch-write-and-allocations-plan.md`](fix-slow-batch-write-and-allocations-plan.md), часть A.0.
**Цель:** определить, где именно «теряется» ~949 мс (avg) / 3.1 с (max) при записи батча: **сервер Postgres**, **клиент/Npgsql**, **схема (партиции/индексы)** или **GC-паузы**.

---

## Факты из кода (уже установлено)

- **Hot-path записи** = `BulkInsertFastAsync(IReadOnlyList<TickData>)` → `SqlTickDataBulkCopy` ([RawTickRepository.cs:31](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:31)):
  ```sql
  INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized)
  SELECT unnest(@ids), unnest(@tickers), unnest(@prices), unnest(@volumes),
         unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)
  ON CONFLICT (ticker, exchange, timestamp) DO NOTHING;
  ```
  Числовые поля идут **бинарно как `numeric[]`** (`NpgsqlDbType.Numeric`) — **без** `::text[]::numeric` (этот каст только в legacy `IEnumerable<RawTick>`-версии, не в hot-path).
- **Схема:** `rawticks` — native partitioned by `timestamp`; **PRIMARY KEY** `(id, timestamp)`; UNIQUE index `IX_rawticks_ticker_exchange_timestamp (ticker, exchange, timestamp)` создать **без partition key** ([миграция:96](src/MarketDataCollector.Infrastructure/Data/Migrations/20260731093821_InitialCreate.cs:96)).
- **Ключевая гипотеза:** `ON CONFLICT (ticker, exchange, timestamp) DO NOTHING` на partitioned-таблице, где конфликтный уникальный индекс **не включает partition key (`timestamp`)** — Postgres не может маршрутизировать к конкретной партиции по `timestamp` для uнік-проверки конфликта. Он обязан проверить конфликт по индексу, разбросанному по под-индексам всех партиций, что для батча 2500 уникальных ключей даёт дорогой обход. Это единственный кандидат на «стабильно ~949 мс» между прогонами.
- **Rest: клиент экранирует** 100% тиков (`batchArray`/`FilteredTickSlice`, reuse-буферы), `ReadAllAsync` + `await foreach` — последовательно. Клиентская часть почти «zero-alloc», аллокации в topN приходят из WS, а не из записи.

---

## Гипотезы (проверить/исключить)

| # | Гипотеза | Признак | Как проверить |
|---|---|---|---|
| H1 | `ON CONFLICT` без partition key → дорогой обход всех партиций | Explain плана INSERT; время approx. независимо от числа строк; страница/seq scan по под-индексам | A0-1 (explain), A0-2 (эксперимент с кол-вом строк) |
| H2 | узкое место в сервере (см. PG-статистика: deadlocks, autovacuum, buffer) | pg_stat_activity/н; vacuum; bloat | A0-1 (stat), A0-2 |
| H3 | узкое место в клиенте/Npgsql (кодирование numeric[]) | время НЕ зависит от объёма данных, флэт по любому числу строк | A0-2 (разные размеры батча) |
| H4 | паузы GC на куче/LOH | корреляция медленных батчей с gc_duration по сэмплам | A0-4 |

---

## Шаги диагностики

### A0-1. Снять фактическую схему и план
Подключиться к БД через Docker-контейнер `marketdata-postgres` (порт 5433):
1. Список партиций `rawticks_*`, их число и диапазоны (`\d+` таблиц/`pg_inherits`).
2. Индексы таблицы (`\di rawticks`), какие под-индексы пересозданы на партициях.
3. `pg_stat_user_tables`/`pg_stat_user_indexes` по интересующему индексу (возможно seq scan).
4. `EXPLAIN (ANALYZE, VERBOSE, BUFFERS)` того же `INSERT ... SELECT unnest` с генератором данных — посмотреть план: ищет ли Postgres партицию по timestamp, как применяет ON CONFLICT.

### A0-2. Измерить время INSERT вручную (без нагрузки)
- Генерировать массивы на 2500 строк (как в проде) и выполнить `INSERT ... SELECT unnest(ARRAY[...]) ... ON CONFLICT DO NOTHING` в psql, замерить тайминг (включить `\timing`).
- Прогнать с **разным числом строк**: 500 / 1500 / 2500 / 5000. Если время ~линейно по числу строк — узкое место сервер/индекс; если флэт — overhead клиента.
- Вариант эксперимента: вставить в **пустую отдельную партицию** (новый timestamp) vs **существующую с данными** — оценить влияние размера партиции/инкса. (Осторожно — не перегружать прод; лучше в тестовой схеме/пакете, и удалить тестовые строки.)

### A0-3. Распределение batch_duration из counters (уже частично есть)
- Из `counter_20260913_021227` выделить `sum/count` по каждому сэмплу, посчитать среднее/max по сэмплам — есть ли «ступени» (все ~одинаковые ≈950 мс) или хвосты.
- Посмотреть, появляются ли скачки в моменты, когда растёт LOH/Gen2 (наложить на A0-4).

### A0-4. Корреляция с GC-паузами
- Из того же CSV взять `gc_duration_nanoseconds_total`, `gc_collections_count_total` (gen2), `gc_allocations_*` по сэмплам.
- Сопоставить с batch_duration по временной оси — если пики батчей совпадают с GC-паузами → часть времени — GC, а не SQL.

### A0-5. Вывод
- Зафиксировать, какая из H1–H4 подтвердилась; обновить [`fix-slow-batch-write-and-allocations-plan.md`](fix-slow-batch-write-and-allocations-plan.md) (какие A.x реально применимы).

---

## Риски/ограничения
- Измерение INSERT **создаёт данные в проде** — откат/очистку тестовых строк делать в том же транзакции или вручную проверить/удалить. Рекомендуется **в отдельной тестовой схеме/партиции** и с `ROLLBACK`-обвязкой, чтобы не засорять `rawticks` и не влиять на нагрузку.
- `EXPLAIN ANALYZE` выполняет реальный INSERT — обернуть в транзакцию с `ROLLBACK`, чтобы не оставить строки.
- Не выполнять параллельно с активным loadtest-прогоном (искажение таймингов).