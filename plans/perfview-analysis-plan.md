# План: PerfView-анализ поверх существующих .nettrace

**Цель.** Закрыть два открытых вопроса из [`counters-analysis_20260807_132018.md`](plans/counters-analysis_20260807_132018.md:155):

1. Источник повышенных аллокаций **~744 Б/тик** — нужен топ горячих типов и call-site аллокаций.
2. Источник **lock-contention ~+2K/прогон** — нужны стеки блокировок.

Подход — **post-processing уже собранных `.nettrace` через PerfView**, без изменения сбора в
[`run_all_profiler.ps1`](run_all_profiler.ps1:1). PerfView умеет открывать файлы `dotnet-trace`
(`.nettrace`) как входной артефакт.

---

## 1. Текущее состояние сбора (факты)

- Сбор выполняет [`tools/Profiler`](tools/Profiler/README.md:1) в режиме `all`
  (см. [`ProfilerOrchestrator.RunAllAsync`](tools/Profiler/Services/ProfilerOrchestrator.cs:62)).
- Trace-профиль по умолчанию = `gc-verbose`. Команда формируется в
  [`TraceCollector.BuildArgs`](tools/Profiler/Core/TraceCollector.cs:94):
  `dotnet-trace collect --process-id {pid} --output ... --profile gc-verbose --duration ...`
- `gc-verbose` содержит события **GC + GCAllocationTick/Allocation** — этого достаточно для
  PerfView-анализа «Allocation Tracking» (по типам/стекам) **без повторного сбора аллокаций**.
- **CPU-событий и stackwalker в gc-verbose нет** → для CPU-профиля нужен профиль
  `cpu-sampling` (уже поддержан скриптом).
- **Contention-событий (0x4000) в gc-verbose нет** → для contention-анализа нужен профиль
  `contention` или `contention-cpu` (уже поддержан скриптом).
- Доступные артефакты в [`traces/`](traces): `allocation_trace_*.nettrace` (прогоны 12:23, 13:02,
  13:17, 13:20), по 2× gcdump и `counters_*.csv`.

---

## 2. Что именно даст PerfView

### 2.1 Аллокации по типам/стекам — на уже собранных `gc-verbose` trace

PerfView может открыть любой `allocation_trace_*.nettrace` (например
`allocation_trace_20260708_132018.nettrace`) и построить:

- **«Allocation by Type»** — топ типов по байтам/штукам за весь прогон.
- **«Allocation by Stack»** — полные стеки мест создания аллокаций → точный список методов в
  hot-path записи/сериализации, создающих `byte[]`/`String` (гипотеза из отчёта §8).

Шаги (GUI):
1. `File → Open` → выбрать `.nettrace`.
2. Дождаться загрузки и пост-обработки;
3. `Memory → Allocation Tracking` (или групповая панель → `Allocs by Kind/Type/Stack`).
4. Сумма по Б/тик — сверить с `gc_allocations` из `counters_*.csv` (ожидание ~744 Б/тик).
5. Загрузить символы: `Symbols → Lookup Symbols` (символ-сервер / PDB Worker).

### 2.2 Contention (локи)

- В `gc-verbose` contention-событий нет → для анализа локов нужно **пересобрать trace с профилем
  `contention`** (или `contention-cpu`).
- Команда уже задана в [`run_all_profiler.ps1`](run_all_profiler.ps1:63):
  ```
  .\run_all_profiler.ps1 -TraceProfile contention-cpu
  ```
  Профиль `contention-cpu` собирает и `contention (0x4000)`, и CPU-сэмплы одновременно — удобно для
  локализации блокировок по стекам (документация в шапке скрипта).
- В PerfView: `Open` trace → `Events` → группа `Contention` → `Contention (stacks)`
  → Show by stack → топ заблокированных/блокирующих методов.

---

## 3. Вариант A — минимум (анализ уже собранных trace, без изменений кода)

1. Выбрать показатель `.nettrace` (13:20 — соответствует разобранному прогону).
2. PerfView → Open → **Allocation Tracking** → топ типов/стеков (закрывает вопрос №1).
3. Для contention — новый прогон скрипта с `-TraceProfile contention-cpu`, затем PerfView →
   **Contention (stacks)** (закрывает вопрос №2).

---

## 4. Вариант Б — встроить post-анализ в `run_all_profiler.ps1` (опционально)

Идеи (уточнить у владельца, т.к. PerfView — GUI-инструмент, CLI-режим ограничен):

- В .NET 8 базовый `dotnet-trace` имеет ограниченную возможность
  `report`/`analyze` (нет полноценного Allocation/Contention по стекам в командной строке).
  Поэтому лучший способ — PerfView (GUI) как ручной post-анализ.
- Рекомендация: **скрипт не модифицировать для PerfView** — сбор уже полный; пост-анализ держать
  ручным (вариант A). В `tools/Profiler/README.md` добавить небольшой раздел «Post-анализ через
  PerfView» — этого достаточно.

---

## 5. Вывод

1. **PerfView нужен**: закрывает то, чего не дают counters — стеки аллокаций и стеки contention.
   Это единственный инструмент в текущем стеке, который даёт call-site по типам на существующем
   `.nettrace`.
2. Метрики первого уровня (дропы/канал/запись/дедупликация) **уже полностью покрыты** —
   PerfView для них не требуется.
3. Для аллокаций **достаточно уже имеющихся `gc-verbose` trace** — PerfView разберёт их пост-фактум.
4. Для contention **нужен отдельный прогон** с `-TraceProfile contention-cpu`
   (текущие дефолтные trace не содержат события 0x4000).

---

## 6. Внедрение (по шагам)

- [ ] Подготовить PerfView (Windows; release с GitHub microsoft/perfview).
- [ ] Открыть в PerfView `traces/allocation_trace_20260708_132018.nettrace`.
- [ ] Загрузить символы (Symbol server / PDB Worker) через `Symbols → Lookup Symbols`.
- [ ] Построить `Allocation Tracking` → зафиксировать топ типов/стеков, сравнить с `~744 Б/тик`
      (закрытие вопроса №1).
- [ ] Для вопроса №2 — прогнать `.\run_all_profiler.ps1 -TraceProfile contention-cpu`
      (при запущенных worker/fake-server), затем в PerfView открыть новый `.nettrace`,
      раздел `Contention (stacks)`.
- [ ] Занести результат в отчёт рядом с counters-отчётами (см. §7).
- [ ] Опционально: добавить описание post-анализа через PerfView в `tools/Profiler/README.md`.

---

## 7. Оформление результата

Итоговый отчёт в стиле `traces/perfview-analysis_<timestamp>.md`, содержание:
- топ-10 типов аллокаций (Байт / Штук / доля) с методами-источниками;
- топ-10 contention-стеков и критические методы;
- вывод + сравнение с историческими ориентирами (`~744 Б/тик`, lock ~+2K).

---

## Задействованные файлы

- [`traces/allocation_trace_20260708_132018.nettrace`](traces/allocation_trace_20260708_132018.nettrace)
  — целевой для анализа аллокаций.
- [`run_all_profiler.ps1`](run_all_profiler.ps1:1) — без изменений (сбор уже покрыт).
- [`tools/Profiler/README.md`](tools/Profiler/README.md:1) — опционально: описание post-анализа PerfView.
- [`plans`](plans) — директория текущего плана.