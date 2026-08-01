---
name: metrics-analysis
description: Анализ Prometheus-метрик и профилировочных данных MarketDataCollector.Worker. Разбирает counters CSV, profiling_report, при необходимости бинарные nettrace/gcdump через dotnet-trace/gcdump. Формирует отчёт в plans/counters-analysis-<timestamp>.md.
author: You
---

# Skill: Анализ метрик MarketDataCollector

Назначение — системный разбор метрик производительности `MarketDataCollector.Worker` по данным, собранным `run_all_metrics.ps1` / `scripts/collect-all.ps1`.

**Основные источники данных (в `traces/`):**
- `counters_<ts>.csv` — Prometheus-метрики по сэмплам (каждые ~5с), включая runtime `.NET` и кастомные метрики `MarketDataCollector`.
- `profiling_report_<ts>.md` — автосводка (размеры файлов, краткий анализ).
- `allocation_trace_<ts>.nettrace` (бинарный) — трассировка аллокаций/CPU.
- `snapshot_peak_<ts>.gcdump` / `snapshot_drained_<ts>.gcdump` (бинарные) — снапшоты кучи на пике и после дренажа.
- `allocation_trace_<ts>.speedscope.json` — конвертированный trace (можно открыть в speedscope).

**Выход:** `plans/counters-analysis-<timestamp>.md` в едином формате существующих отчётов.

---

## Правила работы

1. **Работай от CSV к выводам**, а не наоборот. Числа бери из данных, не из предположений.
2. **Не выдумывай метрики**: используй фактические имена из CSV (колонки `Metric`, `Labels`, `Value`, `Timestamp`).
3. **Помечай аномалии** знаком ⚠️ и всегда давай интерпретацию (что значит и почему важно).
4. **Сравнивай с прошлым прогоном**, если есть предыдущий `counters-analysis-*.md`.
5. Бинарные `nettrace`/`gcdump` разбирай **только при необходимости** (см. шаг 7), по явному запросу или когда counters не дают ответа.
6. Итоговый отчёт сохраняй в `plans/counters-analysis_<yyyyMMdd_HHmmss>.md`.
7. **ВСЕГДА проверяй содержимое `traces/` командой, а не индексацией файлов.** Папка `traces/` содержит большие бинарные файлы (`*.nettrace`, `*.gcdump` по 5–20+ MB, speedscope.json), из-за чего `list_files`/семантический поиск по ней **не работает** (индексация не проходит) и может ошибочно показать папку пустой. Это НЕ значит, что данных нет — они есть, просто не индексируются. Единственный достоверный способ узнать состав `traces/` — выполнить команду `Get-ChildItem` (см. Шаг 1).
8. ⚠️ **Regex-поиск по большим CSV ненадёжен.** По `counters_*.csv` (10+ MB / 10K+ строк, напр. `counters_20260731_093439.csv`) `search_files` возвращал **0 совпадений** по метрикам, которые фактически присутствуют в файле (проверено прямым чтением) — вероятный глюк индексации больших CSV. **НЕ считай «0 совпадений» доказательством отсутствия метрики.** Достоверный способ проверить наличие/значения — **прямое чтение фрагментов** файла (`read_file` с offset/limit) или выборка по колонкам, а не regex по всему файлу (см. Шаг 2).

---

## Шаг 1. Собери контекст прогона

> ⚠️ **ОБЯЗАТЕЛЬНО:** сначала выведи список файлов `traces/` **через команду**, т.к. индексация в этом каталоге не работает (большие бинарные файлы). Используй `execute_command`:
> ```powershell
> Get-ChildItem -Path traces | Select-Object Name, Length, LastWriteTime | Sort-Object LastWriteTime -Descending | Format-Table -AutoSize
> ```
> Если `execute_command` недоступен в текущем наборе инструментов — используй `list_files`, но **пустой результат НЕ является доказательством отсутствия данных**: в `traces/` лежат большие бинарные файлы (`*.nettrace`, `*.gcdump`, `*.speedscope.json`), которые не индексируются. В таком случае продолжай анализ по имеющимся отчётам/CSV и укажи в отчёте, что состав `traces/` не подтверждён командой.
> Если `traces/` нет или он пуст по выводу команды — **остановись и сообщи пользователю**, что данные для анализа отсутствуют, и уточни, где их взять (другая папка / запустить сбор). НЕ делай вывод «нет данных» на основе `list_files`.

- По выводу команды определи актуальный набор файлов в `traces/` (по самой свежей временной метке `_<ts>`): `counters_<ts>.csv`, `profiling_report_<ts>.md`, бинарные файлы.
- Прочитай `profiling_report_<ts>.md`: конфигурация (mode, длительности), размеры файлов, базовые указатели.
- Отметь полный список файлов прогона и их размеры (таблицей в отчёте).
- Если видишь в терминальном логе ошибки (конвертация trace, срыв сбора, остановка процесса) — зафиксируй их как потенциальные аномалии.

## Шаг 2. Прочитай counters CSV

- CSV большой (10+ MB), читай **частями** (`read_file` с offset/limit) или через статистические выборки.
- ⚠️ **Избегай regex-поиска по всему CSV** — он ненадёжен (возвращает «0 совпадений» по фактически присутствующим метрикам, см. правило 8). Проверяй наличие/значения метрик **прямым чтением фрагментов**, а не `search_files`.
- Определи колонки: `Timestamp`, `Metric`, `Labels`, `Value`.
- Собери список уникальных метрик (раздели runtime `.NET` и кастомные `MarketDataCollector`).
- Определи число сэмплов (уникальных `Timestamp`), первый и последний.

## Шаг 3. Динамика конвейера тиков

Для начального, пикового и финального сэмпла сними cumulative-счётчики:

| Метрика | Назначение |
|---|---|
| `ticks_incoming_count_total` | тики на входе конвейера |
| `ticks_received_count_total` | тики, прочитанные из Channel в батч |
| `ticks_processed_count_total` | тики, записанные в БД |
| `ticks_dropped_count_total` | дропы каналом (DropOldest) |
| `ticks_dropped_silently_count_total` | оценка тихих дропов |
| `ticks_deduplicated_cache_count_total` | отсев in-process `DeduplicationCache` внутри батча |
| `ticks_deduplicated_db_count_total` | отсев на уровне БД (`ON CONFLICT DO NOTHING`) |

Расчёты:
- `% входа в батч = received / incoming`.
- `% записи в БД = processed / incoming`.
- `% дропов = dropped / incoming`.
- Пропускная способность входа/записи (тики/сек) по длительности прогона.
- Сверь `incoming - received` с `dropped*` — укажи расхождения.
- **Инвариант баланса тиков** (без учёта редких сбоев записи):
  ```
  received - processed == deduplicated.cache + deduplicated.db
  ```
  Если равенство не выполняется — зафиксируй расхождение ⚠️ и укажи возможные причины (сбои записи, неучтённые дропы).
- Раздели отсев дубликатов на два уровня: сколько отфильтровал кэш в процессе, а сколько — `ON CONFLICT` в БД. Это объясняет разницу `incoming - processed`.

## Шаг 4. Состояние канала и WS

- `processor_channel_backlog_count` (incoming - received) и `processor_channel_fill_count` (histogram `processor.channel.fill` по `channel_index`): оцени бэклог и заполненность канала на старте/пике/финале.
- `processor_batch_channel_fill_count` (histogram `processor.batch_channel.fill`) — **новая метрика** (очередь батчей, ожидающих записи в БД). Появляется в CSV только при фактическом наполнении очереди; не обновляется, когда очередь пуста.
- `ws_active_connections_count` и распределение `ws_messages_received_count_total` по символам (`Labels`).
- Сделай вывод о дисбалансе продюсер/консьюмер и причинах (например, узкое место записи в БД).

## Шаг 5. Длительность записи батчей в БД и дедупликация

- `ticks_batch_write_duration_milliseconds` — гистограмма по `Labels` (`channel_index`, `batch_size`, `inserted_count`).
- Сними показатели по финальным/пиковым сэмплам: `batch_size`, `inserted_count`, `duration`.
- Рассчитай эффективность вставки `inserted/batch` (доля отсеиваемых дубликатов ON CONFLICT).
- Дополнительно свери с новыми счётчиками дедупликации по `Labels` (`channel_index`): `ticks_deduplicated_cache_count_total` (отсев кэшем) и `ticks_deduplicated_db_count_total` (отсев ON CONFLICT). Распределение по каналам позволяет понять, какой канал генерирует больше дубликатов и на каком уровне они отсеиваются.
- Отметь выбросы длительности (медленные батчи) — это критично для дропов.

## Шаг 6. .NET runtime метрики

Сравни первый и последний сэмпл по ключевым runtime-метрикам (колонка `Metric` начинается с `process_runtime_dotnet_*`):

| Метрика | Что смотрим |
|---|---|
| `process_runtime_dotnet_gc_allocations_size_bytes_total` | суммарные аллокации |
| `process_runtime_dotnet_gc_collections_count_total` | счётчики по `gen0/gen1/gen2` |
| `process_runtime_dotnet_gc_heap_size_bytes` | размер кучи (по поколениям в Labels) |
| `process_runtime_dotnet_gc_objects_size_bytes` | размер объектов |
| `process_runtime_dotnet_gc_duration_nanoseconds_total` | суммарное время пауз GC |
| `process_runtime_dotnet_exceptions_count_total` | исключения |
| `process_runtime_dotnet_monitor_lock_contention_count_total` | contention |
| `process_runtime_dotnet_thread_pool_queue_length` | очередь пула |
| `process_runtime_dotnet_thread_pool_threads_count` | потоки пула |
| `process_runtime_dotnet_thread_pool_completed_items_count_total` | завершённые задачи |
| `process_runtime_dotnet_timer_count` | таймеры |

Дай интерпретацию: высокая ли аллокационная нагрузка, много ли Gen2-сборок, растёт ли LOH/heap, есть ли contention.

## Шаг 7. Бинарные файлы (опционально)

Применяй **только если нужно** докопаться до причин (например, высокие аллокации/Gen2/LOH):

- **nettrace** (аллокации): `dotnet-trace report <file>.nettrace analyze -profile-type GCAllocationTick` либо конвертируй и смотри топ аллокаций. В отчёте укажи топ горячих типов (`System.String`, `byte[]`, `KeyValuePair<,>` и др.) и долю.
- **gcdump peak vs drained**: `dotnet-gcdump report <file>.gcdump` для обоих, сравни `Top types by count/size`, Gen2/LOH fragmentation, survivor ratio.

> Не выполняй эти команды, если counters-анализ уже дал исчерпывающий ответ.

## Шаг 8. Аномалии сбора и сравнение

- Зафиксируй технические сбои сбора: недоступность `/metrics`, обрыв CSV, срыв конвертации trace, завершение воркера раньше таймера.
- Найди предыдущий отчёт `counters-analysis-*.md` и построй таблицу сравнения ключевых показателей (вход, записано %, дропы %, max batch duration, Gen2).
- Отметь регрессии/улучшения.

## Шаг 9. Сформируй отчёт

Итоговый `plans/counters-analysis_<ts>.md` со структурой:
1. Заголовок: источник данных, метод сбора, метка времени.
2. Динамика нагрузки и дропы (таблица).
3. **Дедупликация** — разбивка `incoming - processed` на `deduplicated.cache` (отсев кэшем) и `deduplicated.db` (отсев ON CONFLICT), проверка инварианта `received - processed == cache + db` (⚠️ при расхождении).
4. Анализ канала/backlog.
5. WebSocket-подключения (по символам).
6. Длительность записи батчей (таблица + выводы).
7. .NET runtime метрики (таблица начало/конец + интерпретация).
8. Технические аномалии сбора.
9. Сравнение с прошлым прогоном.
10. Ключевые проблемы и рекомендации (нумерованный список, по приоритету).

Каждый раздел — по 1–2 коротких абзаца выводов + таблица с числами. Используй знак ⚠️ для критичных находок.
