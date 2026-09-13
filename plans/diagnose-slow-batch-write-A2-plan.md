# A2-план: диагностика медленной записи батчей (avg 1 047 мс / max 10.1 с)

**Дата:** 13.09.2026
**Основание:** [`counters-analysis_20260913_095700.md`](counters-analysis_20260913_095700.md) — прогон `094702` (1.7M тиков) показал avg batch duration **1 047 мс**, max **10.1 с**, 455K silent-дропов (26.8% входа), backlog канала 455K.

---

## 1. Что уже установлено в ходе диагностики (A2-замер)

### 1.1 Схема БД — партиционирование теперь работает ✅
| Показатель | A0-result (021227) | **094702 (сейчас)** |
|---|---|:---:|
| Куда идут данные | все 1.65M в `rawticks_default` (348 MB) ⚠️ | **в `rawticks_2026_09_13` (122 MB)** ✅ |
| `rawticks_default` | 348 MB | **0 bytes** |
| Итог строк | 1.2M в правильной day-партиции | **1,207,829** (точно = processed) |

> **FakeTickServer исправлен**: `timestamp` теперь реальный (2026-09-13), а не эпоха 1970 — гипотеза «вся нагрузка в одну default-партицию» из A0-result **более неактуальна**.

### 1.2 Серверный INSERT — быстрый (изолированно) 🚀
`EXPLAIN (ANALYZE, BUFFERS)` в заполненную `rawticks_2026_09_13` (1.2M строк), обёрнуто в `BEGIN/ROLLBACK` (безопасно):

| Сценарий | Execution Time |
|---|---|
| INSERT 5000 уникальных ключей, 0 конфликтов | **78.4 мс** |
| INSERT 5000 с потенциальными конфликтами | **93.8 мс** |

**Вывод:** сервер Postgres на batch=5000 быстр (~78–94 мс), индексы/ON CONFLICT не являются узким местом.

### 1.3 Клиентская фазовая разбивка (worker_out.log) — ключевое 🔑
Воркер логирует `prepare` (наш CPU) и `execute` (сеть+БД) для батчей >150 мс:

| Фаза | Диапазон | Доля от total |
|---|---|:---:|
| `prepare` (UUID v7 + сборка массивов) | **0.5–1.7 мс** | ~0.1% |
| `execute` (сеть + БД) | **150–4 000 мс** | **99.9%** |

- **Ранние батчи** (старт прогона): `execute` ≈ **150–500 мс** (avg ~250 мс).
- **Хвост** (после 09:48:04): `execute` растёт **1.2с → 2.0с → 3.99с → 3.34с** (tail=True)`.
- `prepare` ничтожен → **наш CPU НЕ причина**. Всё время — `execute` (Npgsql round-trip + сервер *под конкурентной нагрузкой*).

### 1.4 Разрыв «изолированно vs под нагрузкой»
- Изолированно сервер: **78–94 мс**.
- В прогоне ранние execute: **~250 мс** (×3 от изолированного).
- В прогоне хвостовые execute: **до 4 с (и 10.1 с по counters)**.

> Разрыв ×3–×40 между изолированным и нагрузочным execute — это **деградация под высокой конкурентной записью**, а не фиксированная стоимость. Требует проверки серверной конкуренции/флаша (H2–H4 ниже).

---

## 2. Гипотезы для дальнейшей проверки

| # | Гипотеза | Признак | Как проверить |
|---|---|---|---|
| H2 | **Server WAL/checkpoint флаш** — хвосты от synchronous_commit / fsync при росте партиции | хвостовые execute кратны checkpoint; растут со временем | `pg_stat_activity` wait_event в момент хвостов; `pg_stat_bgwriter` checkpoints_timed; `show synchronous_commit` |
| H3 | **Конкуренция на уникальном индексе** — 3 WS-клиента + writer на одном подключении к БД, lock на `IX_..._ticker_exchange_timestamp_idx` | блокировки `wait_event_type=Lock` на индексе в пике | `pg_locks` в активном окне; pg_stat_database deadlocks |
| H4 | **Autovacuum параллельно с записью** — scan/dead tuple | `rawticks_2026_09_13` autovacuum каждые ? мин; n_dead_tup растёт | `pg_stat_user_tables` за прогон; `vacuum` в pg_stat_activity |
| H5 | **BatchSize 5000 vs 2500** — удвоение батча усилило конфликтную/индексную нагрузку | хвосты в 094702 (5000) тяжелее чем в A0 (2500) | сравнить с прошлым; протестить 2500 |
| H6 | **Npgsql round-trip / EF ExecuteSqlRaw** — кодирование 8×numeric[]/text[] | execute стабильно ~250 мс даже в начале | бенчмарк через сырое NpgsqlConnection без EF; `Npgsql` prepared statements |
| H7 | **`synchronous_commit=on` + медленная подложка Docker Desktop fsync** — каждый COMMIT ждёт fsync WAL до ответа | хвосты execute 1–10 с при интенсивной записи; дисковый флаш в моменты хвостов | `SHOW synchronous_commit` (=on); bенчмарк с `synchronous_commit=off`; iostat на WAL |

> ⚠️ **Уже подтверждено при диагностике:** `synchronous_commit = on` (show). При интенсивном INSERT-потоке каждый батч ждёт fsync WAL. Docker Desktop на Windows — известная медленная подложка для fsync. **Сильный кандидат на хвостовые execute 1–10 с (H2/H7).**

---

## 3. Диагностические шаги

### A2-1. Наблюдение за сервером во время активного прогона
- Запустить параллельно `run_all_profiler.ps1` и в фоне опрашивать:
  ```sql
  SELECT pid, state, wait_event_type, wait_event, now()-xact_start AS xact_age
  FROM pg_stat_activity WHERE datname='MarketDataDb' AND state='active';
  ```
- Ловить момент, когда `execute` в workerOut.log перешагивает 500 мс, и сверять: есть ли `wait_event_type='Lock'` / `IO` / `Client` / `Activity`.

### A2-2. Флаш/checkpoint
```sql
SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, buffers_backend_fsync
FROM pg_stat_bgwriter;
SHOW synchronous_commit;
```
- Если `synchronous_commit=on` и много fsync-хвостов — рассмотреть `synchronous_commit=off` / `local` для тестового стенда.

### A2-3. Конфликтный индекс и locks
```sql
SELECT indexrelname, idx_tup_insert, idx_tup_read, idx_scan FROM pg_stat_user_indexes
WHERE indexrelname LIKE '%ticker_exchange_timestamp%';
```
- В пике: `pg_locks` по режиму `InsertIntent`/`ExclusiveLock` на индексе.

### A2-4. Сравнительный прогон BatchSize 2500 vs 5000
- Временно `MaxBatchSize/MinBatchSize=2500` в `appsettings.LoadTest.json`, пересобрать, прогнать `run_all_profiler.ps1` — если max duration падает, причиной частично является размер батча (усиление конфликтов/round-trip).

### A2-5. Изоляция Npgsql/EF overhead
- Бенчмарк: тот же `INSERT`-массив через **сырое `NpgsqlConnection` + `NpgsqlCommand` с prepared statement** (без EF `ExecuteSqlRawAsync`) vs текущий путь. Если сырой быстрее в разы — оптимизировать хот-путь записи (eschew EF).

---

## 4. Ожидаемые итоги

| Метрика | Текущее (094702) | Цель |
|---|:---:|:---:|
| avg batch duration | 1 047 мс | < 300 мс |
| max batch duration | 10.1 с | < 1.0 с |
| silent-дропы | 455K (26.8%) | 0 |
| backlog канала | 455K | ≤ 40 |

---

## 5. Приоритет гипотез (по влиянию)

1. **H7/H2 (synchronous_commit=on + WAL/checkpoint флаш на Docker Desktop)** — уже подтверждено `synchronous_commit=on`; вероятная причина хвостов до 4–10 с. Проверить первым: бенчмарк с `synchronous_commit=off` (A2-2) + наблюдение wait_event (A2-1).
2. **H4 (autovacuum параллельно с записью)** — быстро проверить через pg_stat_activity (A2-1).
3. **H5 (BatchSize 5000)** — простой конфиг-эксперимент (A2-4): реальный риск усиления индекса.
4. **H6 (Npgsql/EF overhead)** — объясняет стабильные ~250 мс в начале (A2-5).
5. **H3 (lock на уникальном индексе)** — проверить через pg_locks в пике (A2-3).
6. H1 (клиентский CPU prepare) — **уже опровергнута** (0.5–1.7 мс), не трогать.

---

## 6. Риски/ограничения
- `EXPLAIN ANALYZE` с реальным INSERT — **только в `BEGIN; ...; ROLLBACK;`** с уникальным маркером (`exchange='a0diag'`), чтобы не мутировать `rawticks` (урок из A0-result).
- Наблюдение pg_stat_activity/locks требует активного loadtest-прогона → **только по согласованию** (правило: явный запрос перед запуском FakeTickServer/нагрузки).
- Сравнительный прогон BatchSize меняет конфиг → запросить отдельный прогон.