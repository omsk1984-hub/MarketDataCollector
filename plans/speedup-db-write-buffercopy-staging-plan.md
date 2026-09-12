# План: ускорение записи в БД (кэш массивов + BINARY COPY в staging)

Дата: 12.09.2026
Основание: прогон `start_loadtest.ps1` (2026-09-12 19:49) — консьюмер записи (~20-23к/с) не успевает за входом (~25-31к/с), канал переполняется и `DropOldest` теряет ~353к тиков (79.3% записано). Требуется поднять пропускную способность записи.

---

## Контекст / что уже есть

Hot path записи (в проде, `UseSingleConsumer=true`):
- [`MarketDataProcessor.ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:888) → `repository.BulkInsertFastAsync(filteredSlice, _timeService, cancellationToken)` — это перегрузка `IReadOnlyList<TickData>`, а НЕ `IEnumerable<RawTick>`.
- [`RawTickRepository.BulkInsertFastAsync(IReadOnlyList<TickData>, ...)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) **уже** кэширует массивы (`ReusableArrayCache` + предзаполненный `NpgsqlParameter[]`), пишет одним `INSERT ... SELECT unnest(@arr) ... ON CONFLICT DO NOTHING`.
- Перегрузка [`BulkInsertFastAsync(IEnumerable<RawTick>, ...)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:326) **в боевом пайплайне не вызывается** — её использует только бенчмарк [`TickWriteBenchmark.BenchmarkRunner`](tests/TickWriteBenchmark/BenchmarkRunner.cs:142).

Вывод: **задача №1 (кэш массивов в `IEnumerable<RawTick>`) НЕ ускорит боевую запись** — это оптимизация бенчмарка. Задача №2 (BINARY COPY) ускорит реальный hot path.

---

## Задача №1. Кэш массивов в `BulkInsertFastAsync(IEnumerable<RawTick>, ...)`

Цель: устранить аллокации `new Guid[count]`/`new string[count]`/`ToString()` per batch в бенчмарке (и «на всякий случай» при будущем использовании). Низкий риск, локально.

### Изменения

**Файл:** [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs)

1. Добавить 8 кэш-полей рядом с `_idsCache` (строки 43-50), того же типа `ReusableArrayCache<T>`:
   - `_rawIdsCache` (Guid), `_rawTickersCache` (string), `_rawPricesCache` (string), `_rawVolumesCache` (string), `_rawTimestampsCache` (DateTime), `_rawExchangesCache` (string), `_rawReceivedAtsCache` (DateTime), `_rawNormalizedsCache` (bool).

   Примечание: эта версия принимает `decimal` и в коде делает `e.Price.ToString(CultureInfo.InvariantCulture)` (строки 351-352). `string[]` с `ToString()` здесь остаётся необходим (в отличие от `TickData`-версии, где `decimal[]`/`Numeric`) — см. SQL ниже. Поэтому кэшируем `string[]`.

2. Добавить предсозданный `NpgsqlParameter[]` (по аналогии с `_tickDataParameters`, строки 59-69), но с типами:
   - `@prices`/`@volumes` → `NpgsqlDbType.Array | NpgsqlDbType.Text` (string[]), а в SQL — каст `::text[]::numeric` (как уже сделано в строке 361).

3. В теле метода (строки 337-374) заменить `new Guid[count]`/`new string[count]`/... на `REUSABLE.Rent(count)`; в цикле заполнять кэшируемые массивы; обновлять `Value` у предсозданного `NpgsqlParameter[]` вместо создания нового.

   ⚠️ Важно: после `ExecuteSqlRawAsync` Npgsql **не должен** асинхронно читать те же массивы после возврата. Так как consumer последовательный (Scoped), массивы безопасно переиспользовать в следующем батче.

4. Retry-цикл оставить как есть (строки 377-400).

### Риски
- Низкий: поведение идентично, меняется только источник массивов/параметров.
- Npgsql требует `Array.Length == count` — `ReusableArrayCache.Rent(count)` возвращает точный размер (строки 222-249 уже это гарантируют).

### Ожидаемый эффект
- Бенчмарк: устранение ~8 аллокаций/батч и строковых `ToString()` (там, где остаются String dec) — снижение GC-давления Gen2 в бенчмарк-сценарии. Боевой throughput не изменится (метод не в hot path).

---

## Задача №2. BINARY COPY в staging-таблицу (основной выигрыш)

Цель: заменить UNNEST-INSERT (один накладный INSERT с routing/индексами на каждый батч) на **быстрый COPY в unlogged staging-таблицу без индексов + один merge-INSERT `ON CONFLICT DO NOTHING`**. Ожидаемый прирост ×2-3.

### Ключевое ограничение
- `COPY FROM STDIN (FORMAT BINARY)` доступен только через сырое подключение `NpgsqlConnection.BeginBinaryImport()`.
- [`BulkInsertFastAsync(IReadOnlyList<TickData>, ...)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:446) использует EF `_context.Database.ExecuteSqlRawAsync` (DbContext), который **не умеет COPY**.
- Нужен доступ к драйверу Npgsql поверх EF. В проекте не найден готовый фабричный доступ к raw-подключению — надо добавить (см. шаг 2.1).

### Шаги

**2.1. Доступ к сырому `NpgsqlConnection` из `RawTickRepository`**
- Добавить зависимость на нечто, что умеет отдавать DTC/`NpgsqlConnection` по `MarketDataDb`. Кандидаты:
  - (a) `NpgsqlPool`/connection string из конфига (`ConnectionStrings.MarketDataDb`), открывая отдельное соединение в `${WORKER}`-скопе. Проект уже собирает conn-строку (см. `appsettings.json:48`).
  - (b) EF `_context.Database` — получить raw driver: для `Npgsql` через `(context as NativeDatabaseWrapper).unwrap(_ => _)` если реализовано; иначе — вариант (a).
- Рекомендация: **(a)** — открывать `NpgsqlConnection` по conn-строке в каждый вызов `BulkInsertFastAsync(TickData)` и возвращать в пул (или держать один per repository при `UseSingleConsumer`). Использовать `NpgsqlPool` из `Npgsql` для переиспользования подключений.

**2.2. staging-таблица (постоянная, без DDL в hot path)**
- Создать на старте (миграция `20260912_..._CreateRawTicksStaging`): обычную таблицу-копию `rawticks` **без** партиционирования, **без** уникальных индексов и PK-индекса — только столбцы (id, ticker, price, volume, timestamp, exchange, receivedat, normalized).
- Таблица **постоянная** (не temp, не unlogged? см. ниже), чтобы не платить DROP/CREATE каждый батч (минус прежнего подхода, описанный в строках 307-319).
- Перед каждым батчем — `TRUNCATE staging` (быстрый, без точек вакуума на самой таблице, т.к. данных нет).

   О выборе unlogged: `UNLOGGED` нельзя по таблице, участвующей в копировании как обычная (unlogged — это про WAL отдельных таблиц). Для staging стоит **отключить WAL-replication не обязательно**; главное — нет индексов и нет FK. Если надо ещё быстрее — `synchronous_commit=off` на сессии COPY (см. рекомендации в конце). Для Unlogged можно использовать отдельную tablespace/специальную таблицу — **не усложняем**, оставляем обычную без индексов.

**2.3. Модификация `BulkInsertFastAsync(IReadOnlyList<TickData>, ...)` (строка 446)**
- Вместо `Insert ... SELECT unnest` — новый flow:
  1. `TRUNCATE rawticks_staging` (1 round-trip, ~быстро).
  2. `conn.BeginBinaryImport("COPY rawticks_staging (...columns...) FROM STDIN (FORMAT BINARY)")` — писать строки из `ticks` (UUID v7, ticker, price, volume, timestamp, exchange, receivedat, normalized). Npgsql пишет бинарный COPY без больших `byte[]` (убирает LOH-источник).
  3. `writer.CompleteAsync()`.
  4. merge одним запросом:
     ```sql
     INSERT INTO rawticks (id, ticker, price, volume, timestamp, exchange, receivedat, normalized)
     SELECT id, ticker, price, volume, timestamp, exchange, receivedat, normalized
     FROM rawticks_staging
     ON CONFLICT (ticker, exchange, timestamp) DO NOTHING;
     ```
     (это тот самый путь, что уже используется в `SqlTickDataBulkCopy`, только источник — staging).
  5. Оборачивать шаги 1-4 в одну транзакцию (если не применяем COPY вне транзакции для скорости) — учесть, что `BeginBinaryImport` работает в рамках соединения.

- Retry-цикл: перенести на весь блок (или на merge-запрос). Транзиентные сбои COPY/merge — retry с backoff, как сейчас (строки 491-520).

**2.4. Кол-во тиков/батч**
- При COPY больше накладных меньше; батч можно увеличить до 5000-10000 (см. `MarketDataProcessorOptions`: сейчас 2500 в `appsettings.json:53-54`). Отдельно проверить на прогоне.

### Риски / что учесть
- **Правильность дедупликации**: COPY не знает про `ON CONFLICT` — если в `rawticks` уже есть строка с тем же `(ticker, exchange, timestamp)`, COPY вставит дубль в staging, а merge отсеет. Это нормально (merge делает дедуп). Но важно, что staging без уникального индекса — дубли в staging допустимы, они отсеются при merge.
   - ⚠️ В staging при COPY **дубликаты внутри одного батча не сжимаются** (COPY не дедупит). Если в батч попадают повторные `(ticker, exchange, timestamp)` — правильность сохранится (merge ON CONFLICT отсеет), но merge поставит дубли → небольшой лишний объём. Т.к. `DeduplicationCache` уже отсеивает почти всё (`deduplicated_db = 1` за прогон), это не проблема.
- **Доступ к raw-подключению** — главный технический пункт; без него задача не реализуема через EF. Решение 2.1(a) минимизирует объём.
- **Партиционирование**: `rawticks` партиционирована (`PartitionMaintenanceService`). Merge INSERT через `ON CONFLICT` с триггерами/индексами на partitioned parent может замедлиться. Проверить на реальном прогоне; при необходимости — ставить `partition_key`/роутинг как в обычном UNNEST.
- **`synchronous_commit=off`** на COPY-сессии для доп. прироста (с оговоркой durability — ок для теста/витрины).
- **Провал Npgsql `Buffer requirements for format not respected`** для `Numeric` в массивах — при COPY этот путь не используется (пишем напрямую значения), что **обходит** этот известный баг (см. [`fix-npgsql-buffer-format-error-plan.md`](plans/fix-npgsql-buffer-format-error-plan.md)).

---

## Порядок работ

1. **Задача №1** (кэш в `IEnumerable<RawTick>`) — самостоятельна, низкий риск, для бенчмарка. Если хочется быстрее к цели — можно пропустить/сделать после №2.
2. **Задача №2**:
   - 2.1 доступ к raw Npgsql (наиболее технический риск) — прототип в `TickWriteBenchmark` (там уже есть `BinaryCopyDirectChunk` с `BeginBinaryImport`), затем перенос в репозиторий.
   - 2.2 миграция staging-таблицы.
   - 2.3 переписать горячий метод.
3. **Проверка**: unit-тесты `RawTickRepositoryTests` + `RawTickRepositoryDeadlockTests`; затем прогон `start_loadtest.ps1` (только после явного подтверждения пользователя) и сверка метрик (`ticks_batch_write_duration`, дропы, throughput).

## Критерии успеха
- Прогон 1.7M тиков @ 25k/с без дропов, либо дропы ≤1%.
- `ticks_batch_write_duration` медиана заметно ниже (~50-60 мс на 2500, или ×2-3 throughput).
- Отсутствие регрессий в дедупликации (`deduplicated_db` остаётся ≪ batch).

## Открытые вопросы (уточнить у пользователя/в прогоне)
- Подтвердить доступ к raw Npgsql-подключению (вариант a: отдельный пул по conn-строке).
- Можно ли использовать `synchronous_commit=off` (durability-off trade-off) в load-тесте.
- Целевой Rps: если цель «0 дропов» — также может потребоваться снижение Rps до ~20-23к/с, независимо от ускорения записи (запас должен остаться).