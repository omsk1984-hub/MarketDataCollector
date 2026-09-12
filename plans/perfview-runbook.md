# Runbook: PerfView — глубокий анализ trace (аллокации + contention)

**Назначение.** Закрыть вопросы, которые не покрывает CLI-шаг `dotnet-trace report topN`
(см. [`plans/perfview-analysis-plan.md`](plans/perfview-analysis-plan.md)):

1. Источник повышенных аллокаций **~744 Б/тик** — нужен топ типов и call-site аллокаций.
2. Источник **lock-contention ~+2K/прогон** — нужны стеки блокировок.

PerfView — единственный инструмент в текущем стеке, который на существующих `.nettrace`
даёт стеки аллокаций по типам и contention-стеки.

---

## 0. Подготовка

- Скачать PerfView (Windows): <https://github.com/microsoft/perfview/releases> (файл `PerfView.exe`).
- Запустить от **администратора** (для загрузки символов и корректного чтения событий).
- Убедиться, что доступен символ-сервер (Microsoft Symbol Server / локальные PDB Worker).

---

## 1. Аллокации по типам/стекам — на уже собранном `gc-verbose` trace

Дефолтный профиль `gc-verbose` уже содержит события GC + GCAllocationTick/Allocation,
чтобы PerfView построил Allocation Tracking **без повторного сбора**.

Целевой файл (пример из прогона 13:20):
`traces/allocation_trace_20260807_132018.nettrace`

Шаги:
1. `File → Open` → выбрать `.nettrace`.
2. Дождаться окончания обработки (статус-бар «Processing» исчезнет).
3. Загрузить символы: `Symbols → Lookup Symbols` (дождаться завершения).
4. `Memory → Allocation Tracking` (или дерево групп `Memory / Allocs`).
5. Сгруппировать:
   - `MemoryGroupByType` — топ аллоцируемых типов (Байт / Штук);
   - `MemoryGroupByStack` / `Callers` — полные стеки мест создания аллокаций.
6. Сверить итог по байтам с `gc_allocations_size_bytes_total` из `counters_*.csv`
   (для прогона 13:20 сумма ≈ 1 266 МБ / 1.7M тиков ≈ **744 Б/тик**).

Ожидаемый результат — незакрываемые hotspot-методы: какие методы в hot-path записи/сериализации
создают `byte[]`/`String`, и насколько их долю в общем объёме аллеркаций.

---

## 2. Contention (локи) — нужен отдельный trace с событиями 0x4000

Дефолтный `gc-verbose` не содержит contention-событий. Нужен trace с профилем
`contention` или `contention-cpu`.

Команда (при запущенных worker/fake-server):
```powershell
.\run_all_profiler.ps1 -TraceProfile contention-cpu
```

Профиль `contention-cpu` собирает и contention (0x4000), и CPU-сэмплы одновременно — это
позволит локализовать блокировки по стекам в той же точке.

Шаги в PerfView:
1. `File → Open` → новый `.nettrace`.
2. `Events` → группа `Contention` → подпункт `Contention (stacks)`.
3. `Show by callstack` (`Show` → `CallTree`/`By stack`).
4. Топ стелс:
   - biggest time в заблокированном состоянии (желтые фреймы = событие блокировки);
   - понять блокирующий метод (обычно фрейм с `lock`/`Monitor`/`SpinWait`).
5. При необходимости усреднённо cross-check с `monitor_lock_contention_count_total`
   из `counters_*.csv` (для 13:20 — +1 940).

---

## 2.5 (один прогон для обоих) — contention-cpu + аллокации

Если нужен и Allocation, и Contention в одном trace, использовать профиль `contention-cpu`
и в PerfView:
- для аллокаций — `Memory → Allocation Tracking` (runsно всё равно есть GCAllocationTick в trace);
- для lock — `Events → Contention...`.

---

## 3. Составление итогового отчёта

Создай `traces/perfview-analysis_<timestamp>.md`, содержащий:
1. `### Топ-10 типов аллокаций` — таблица (Тип / Байты / Штук / доля) + методы-источники.
2. `### Топ-10 contention-стеков` — таблица (Метод блокировки / Время блокировки / Стек).
3. Вывод:
   - источник высоких аллеркаций и рекомендация (например: переиuse буферов/`ArrayPool`, см. планы
     `allocation-reduction-*`);
   - источник блокировок (например, `lock` в hot path обработки батчей);
   - сравнение с историческими ориентирами (`~744 Б/тик`, lock ~+2K).
4. Ссылки на задействованные `.nettrace`.

---

## 4. Связка с `run_all_profiler.ps1`

CLI-шаг (`*_topn.md`) даёт только CPU top. Для аллеркаций и будь contention обязательно
использовать PerfView по этому runbook. Оба уровня анализа дополняют друг друга:
- `counters_*.csv` — здоровье конвейера (дропы/канал/запись);
- `*_topn.md` — топ CPU, автоматически;
- PerfView — аллоцирование по типам/стекам и contention-стеках, вручную.

---

## Быстрые команды

```powershell
# обновленный Profiler с topN (сборка уже с новым шагом)
.\tools\Profiler\compile.ps1

# прогон с contention-cpu (для PerfView contention-анализа)
.\run_all_profiler.ps1 -TraceProfile contention-cpu

# CLI topN отдельно по существующему trace
dotnet-trace report "traces\allocation_trace_20260807_132018.nettrace" topN -n 20 --inclusive