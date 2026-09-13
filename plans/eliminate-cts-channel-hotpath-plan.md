# План: устранение `CancellationTokenSource.*` из hot-path чтения канала (~57% topN)

> **СТАТУС: РЕАЛИЗОВАНО (2026-09-13).** Внедрён **вариант B (гистерезис)** вместо варианта A:
> варианта A требовал выноса локального состояния (`batchArray`/`Count`) в фоновый таймер, что несло
> гонки `TryReset` (откачены ранее). Поэтому применён гистерезис тишины: в плотном hot-path ожидание
> идёт через короткий опрос `Task.Delay` без токена; таймерный флаш `CancelAfter` включается только
> после `FlushTimerHysteresisThreshold = 8` подряд пустых опросов канала (реальная тишина). В `else`-ветке
> (обычное ожидание) `WaitToReadAsync(cancellationToken)` заменён на `WaitToReadAsync(CancellationToken.None)`
> — отмена при остановке обеспечивается `TryComplete()` канала в `StopProcessingAsync`.
> **Результат:** `dotnet build` — 0 ошибок; полный прогон тестов **280 passed / 0 failed**
> (включая `MarketDataProcessorTests.FlushTimer_*`, остановку канала, Kafka-интеграцию).
> Нагрузочная верификация — по отдельному согласованию (правило проекта).

**Дата:** 2026-09-13
**Источник:** `plans/counters-analysis_20260913_104057.md` — рекомендации №1 (и №3 — как следствие contention).
**Прогон-фиксатор (104057):** топN exclusive: `CancellationTokenSource.Register` **20.59%** + `CancelAfter` **15.28%** + `WaitToReadAsync` **11.64%** + `CreateLinkedTokenSource` **9.44%** ≈ **~57%** CPU · lock contention **2,480**.
**Baseline (045558):** суммарно ~40%. → Регрессия по доле на фоне полной загрузки консьюмера.

---

## 1. Диагноз (по данным прогона 104057)

Ранее (план `cts-hotpath-optimization-plan.md`) WS receive был переведён на `CancellationToken.None` — и это сработало: `ManagedWebSocket.ReceiveAsync` снизился до 8.09%, `GetReceiveResult` до 8.36%. **Новый #1 источник — горячий цикл чтения канала в `MarketDataProcessor`.**

Стоп-условие цикла после `TryRead`-дренажа:
```java
// MarketDataProcessor.cs:434 (CollectorLoopAsync) и :681 (ProcessBatchesAsync)
if (_flushIntervalSeconds > 0 && batchCount > 0 && channel.Reader.Count == 0)
{
    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    linkedCts.CancelAfter(TimeSpan.FromSeconds(_flushIntervalSeconds));
    var hasData = await channel.Reader.WaitToReadAsync(linkedCts.Token)...
}
```

**Почему это и есть main hot path, а не «паузы продюсера»:**
- При полной нагрузке (backlog = 0) консьюмер `while (TryRead)` вычитывает канал до `Count == 0` **на каждой итерации**, затем условие `Count == 0 && batchCount > 0` истинно.
- Создаётся свежий `linkedCts` + таймер `CancelAfter(_flushIntervalSeconds)` + регистрация на токен, даже если канал наполнится через микросекунды и `await` сразу вернётся.
- Каждый вызов = `CreateLinkedTokenSource` (9.4%) + `CancelAfter` (15.3%, создание/уничтожение таймера) + `Register` (20.6%, регистрация на каждое ожидание) + `WaitToReadAsync` (11.6%).
- Суммарно ~57% exclusive — **основной источник CPU и contention при 100%-обработке потока**.

> Это отличает прогон 104057 (он обработал все 1.7M, поэтому channel hot path засветился) от 094702 (где bottleneck был в записи БД и channel-путь простаивал).

**Почему нельзя просто реюзать `CancelAfter`+`TryReset()`:** откачено ранее (`cts-hotpath-optimization-plan.md` §2.3) — `TryReset()` не переводит сгоревший от таймера CTS обратно в активное, что даёт бесконечный цикл при `StopProcessingAsync` (тест `FlushTimer_FlushesPartialBatch_OnTimerTick` завис).

---

## 2. Целевое решение (вариант A — рекомендуется)

**Убрать таймерный `CancelAfter` из горячего пути чтения канала** и ждать данные через `WaitToReadAsync(CancellationToken.None)`, а флаш частичного батча по таймеру вынести в **один фоновый периодический flush** вместо создания таймера на каждую итерацию.

### 2.1 [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:434) и [:681](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:681) — точечная замена ветки тишины
```java
// ВМЕСТО CreateLinkedTokenSource + CancelAfter + WaitToReadAsync(linkedToken):
// В плотном входе канал наполнится сам → await вернётся мгновенно.
// При реальной тишине / остановке канал завершится через TryComplete (StopProcessingAsync)
// и WaitToReadAsync вернёт false → goto channelCompleted.
if (!await channel.Reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
{
    goto channelCompleted;
}
```
- Полностью убирает из горячего цикла `CreateLinkedTokenSource`, `CancelAfter` и регистрацию на токен.
- `CancellationToken.None` — SDK не регистрирует callback отмены, только ожидает появления данных/завершения канала.
- Отмена/остановка обрабатывается **снаружи через `TryComplete()`** канала (улья уже в `StopProcessingAsync`: single-consumer [:327](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:327), multi [:361](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:361)) — пробуждает `WaitToReadAsync` → `false` → корректный выход.

### 2.2 Флаг таймерного флаша — отдельный фоновый периодический flush
Чтобы не потерять флаш частичного батча при долгой тишине канала (partial batch не зависал) — вынести таймер в **одну** периодическую задачу на `_flushIntervalSeconds` (на источник, а не на итерацию):

- В `StartProcessingAsync` запустить `_flushTask = FlushTimerLoopAsync(channelIndex, cancellationToken)` — цикл `WaitDelayAsync(_flushIntervalSeconds)`, затем, если `channel.Reader.Count == 0 && batchCount > 0` (через shared flag/`AtomicInt` `_pendingBatchCount[channelIndex]`), отправляет partial batch в `_batchChannel`.
- Консьюмер при `batchCount > 0 && Count == 0` просто ждёт `WaitToReadAsync(None)`; если данные пришли — `readTicks`, если тишина — фоновый таймер делает flush.
- Остановка таймера — по общему `cancellationToken`/`_internalCts` + join в `StopProcessingAsync`.

> **Защита от «флакю»:** не используется `TryReset`. Гарантия отсутствия двойного флаша — shared atomic `_pendingBatchCount` (один owner) и проверка `Count == 0` перед отправкой. Это решает известную проблему отката иначе — без переиспользования сгоревшего CTS.

### 2.3 Обновить `StopProcessingAsync` [:301](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:301)
- Отменить и дождаться `_flushTask` перед/параллельно с `TryComplete()` канала, чтобы не гоняться за финальным partal flush.

---

## 3. Альтернативы (если A крупноват для scope)

### Вариант B — минимальный (гистерезис тишины)
Не выносить таймер, но разделить плотный случай:
- если `Count == 0 && batchCount > 0` и при этом **последний дренаж был быстрым** (продюсер активен) → `WaitToReadAsync(CancellationToken.None)` без таймера;
- `linkedCts + CancelAfter` создавать **только** после N подряд пустых ожиданий (эвристика «реальная тишина»), где таймер действительно нужен.
- Эффект меньше (всё ещё создаёт токен при установлении тишины), но переписывание кода минимально и рискованность ниже.

### Вариант C — сначала измерения (перед рефакторингом)
- Прогнать PerfView/contention-stack по прогону 104057 и убедиться, что именно `WaitToReadAsync(linkedCts.Token)` — стек `Register` (а не, например, `SendAsync`/телеметрия `EventSource` 6.68%). Быстровоспроизводимо, уточняет scope, прежде чем менять дизайн флаша.

> Вероятный выбор — **A** (полностью убирает источник), с обязательным C-подтверждением стека для телесности.

---

## 4. Верификация без нагрузки

1. `dotnet format` (правило проекта) по изменённым файлам.
2. Полный прогон юнит-тестов:
   - `MarketDataProcessorTests` — **фокус**: `FlushTimer_FlushesPartialBatch_OnTimerTick`, `FlushTimer_MultipleTimerTicks_OnlyOneFlush`, закрытие канала при `StopProcessingAsync`, отсутствие обязательного зависания (таймаут 15с);
   - добавленные/обновлённые тесты: фоновый таймерный flush сбрасывает partial batch при тишине; `StopProcessingAsync` завершается без гонок; нет `TryReset`-зацикливания;
   - регресс WS/остальных консьюмеров.

## 5. Нагрузочный прогон (по явному согласованию с пользователем)

`run_all_profiler.ps1` (режим `all`, FakeTickServer) с нагрузкой 1.7M тиков (как в baseline).

**Критерии успеха:**

| Метрика | Baseline 045558 | 104057 (сейчас) | Ожидание после A |
|---|:---:|:---:|:---:|
| `CancellationTokenSource.Register` | 17.37% | 20.59% | **≈0%** (вне таймерного flush) |
| `CancelAfter` | 10.6% | 15.28% | **≈0%** |
| `CreateLinkedTokenSource` | 12.13% | 9.44% | **≈0%** |
| `WaitToReadAsync` | — | 11.64% | ниже |
| Lock contention | 2,392 | 2,480 | **существенно ниже** |
| Аллокации на тик | ~619 Б | ~598 Б | ниже |
| Дропы / backlog / % записи | 0 / 0 / 97% | 0 / 0 / 97% | **без регрессии** (сохранение) |
| Timer count (runtime) | 17 | 16 | стабильно (фоновый таймер 1) |

## 6. Риски

| Риск | Оценка | Митигация |
|------|:---:|---|
| Потеря флаша partial batch при тишине (был на `CancelAfter`) | Средняя | Фоновый периодический flush (вариант A) или гистерезис (B) сохраняет поведение; единственный owner `_pendingBatchCount` + проверка `Count==0` не даёт двойного флаша. |
| `WaitToReadAsync(None)` не пробуждён при `StopProcessingAsync`, если канал не завершён TryComplete | Средняя | Стоп уже вызывает `TryComplete()` до ожидания `_processingTask` (single :327 / multi :361) → `WaitToReadAsync` вернёт `false`. Держать этот порядок. |
| Фоновый flush-таймер гоняется с консьюмером (двойная отправка одних данных) | Средняя | `_pendingBatchCount` атомарно обнуляется только владельцем; перед отправкой — проверка `Count == 0`; таймер отменяется в `StopProcessingAsync` до закрытия канала. |
| Отказ от регистрации на токен ломает тест, ожидающий `OperationCanceledException` при отмене | Низкая | Ветка тишины не должна зависеть от токена для корректной остановки (отмена через TryComplete). Если тест завязан на токен — обновить на проверку `channelCompleted`.

## 7. Файлы

| Файл | Действие |
|------|----------|
| [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) | ветки тишины `CollectorLoopAsync` (:443) и `ProcessBatchesAsync` (:690) → `WaitToReadAsync(None)`; добавить `_flushTask` + `_pendingBatchCount`; `StopProcessingAsync` — join flush-таймера |
| `tests/MarketDataCollector.Tests/.../MarketDataProcessorTests.cs` | обновить/добавить тесты фонового flush и остановки |

## 8. Итог ожидаемый

- Снятие ~40–57% topN CPU (`CancellationTokenSource.*`) с горячего пути чтения канала при полной нагрузке.
- Снижение lock contention (регистрация отмены на каждое ожидание уходит) и аллокаций на тик.
- Без регрессии по дропам/backlog/% записи (инвариант `received−processed == dedup` сохраняется).
- Нагрузочная верификация — по отдельному согласованию.