# План: устранение hot-path аллокаций/CPU `CancellationTokenSource` (~40%)

**Дата:** 2026-09-13
**Источник:** `plans/counters-analysis_20260913_045558.md` — пункт 2 рекомендаций.
**Baseline (045558):** `CancellationTokenSource.Register` 17.37% + `CreateLinkedTokenSource` 12.13% + `CancelAfter` 10.6% ≈ **~40% exclusive**; contention 215→2392; ~619 Б/тик.

---

## 1. Диагноз

Основной источник — **регистрация токенов отмены в async-стеках** на каждый вызов в двух местах:

### 1.1 WebSocket receive hot path (главный — ~17–19% `Register` + lock contention)
- [`WebSocketMessageReceiver.RunLoopCoreAsync`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:102) вызывает `_connectionManager.ReceiveAsync(readSegment, cancellationToken)` на **каждое WS-сообщение** (~21K/с × 3 соединения).
- `ManagedWebSocket.ReceiveAsync` (SDK) при передаче живого токена **регистрирует callback отмены на каждый вызов** через `CancellationTokenSource.Register`, что включает внутренний `lock` → это и есть `Register` 17–19% CPU и рост contention.
- Отмена receive при остановке loop уже обеспечивается **снаружи**: `StopReceiveLoopAsync()` отменяет `_loopCts` и `_messageReceiver.StopReceiveLoopAsync()` дожидается завершения loop (см. `BaseWebSocketClient.StopReceiveLoopAsync` [стр. 296]). `OperationCanceledException` ловится в `RunLoopCoreAsync` (стр. 157). **Токен в receive не нужен для корректной остановки.**

### 1.2 Flush-таймаут ожидания канала (вторично — `CreateLinkedTokenSource`+`CancelAfter`)
- [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:443) и [`ProcessBatchesAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:690) создают `CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)` + `CancelAfter(...)` на **каждую итерацию флаша** при тишине канала.
- Срабатывает только когда `channel.Reader.Count == 0` (тишина) — в плотном сценарии вторично, но при паузах продюсера генерирует тысячи linked CTS.

> Примечание: в прогоне `031212` CTC уже уходил к ~2% (механика планировщика). Цель — сделать глобально ненадёжным этот источник независимо от планировщика.

---

## 2. Изменения

### 2.1 [WebSocketConnectionManager.cs](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:107) — receive на `CancellationToken.None`
`ReceiveAsync` — тривиальная не-async обёртка над сокетом. Передаём `CancellationToken.None` вместо входящего токена для операции чтения (отмена приёма осуществляется сверху через `StopReceiveLoop`).

### 2.2 [WebSocketMessageReceiver.cs](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:102) — оба receive-вызова на `CancellationToken.None`
- Основной `ReceiveAsync` (стр. 102);
- oversize-skip цикл (стр. 121).
Токен остаётся для `Task.Delay` (non-hot) и проверок `IsCancellationRequested`.

### 2.3 [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:443) — НЕ изменяется (откат)

**Попытка переиспользуемого CTS отклонена.** В `CollectorLoopAsync`/`ProcessBatchesAsync` был применён переиспользуемый `flushCts` (`TryReset()+CancelAfter()` вместо `CreateLinkedTokenSource` на итерацию) с целью убрать 12% `CreateLinkedTokenSource`.

**Причина отката** — ненадёжность `CancellationTokenSource.TryReset()`:
- после срабатывания `CancelAfter()` (сгорел таймер) CTS переходит в состояние "cancel requested" необратимо; `TryReset()` не переводит его обратно в активное;
- при `StopProcessingAsync` (graceful shutdown через `_channels[0].Writer.TryComplete()`) коллектор в ветке `batchCount > 0 && Count == 0` с не-сброшенным `flushCts` попадал в цикл: `WaitToReadAsync(flushCts.Token)` немедленно бросал `OperationCanceledException` → catch → `continue` → снова та же ветка → **бесконечный цикл**, игнорирующий `TryComplete` → тест `FlushTimer_FlushesPartialBatch_OnTimerTick` завис (timeout 15s).
- Заметка: `FlushTimer_MultipleTimerTicks_OnlyOneFlush` прошёл с той же правкой — поведение зависит от тайминга (гонка между срабатыванием таймера и `StopProcessingAsync`), т.е. правка **флакю-нестабильна** и недопустима в проде.

**Решение:** оставить исходный `CreateLinkedTokenSource` + `CancelAfter` в MDP. Этот источник (`~12%` + `~10.6% CancelAfter`) срабатывает **только при тишине канала** (`Count == 0`), т.е. НЕ в плотном hot path, а в паузах пополнения. Приоритет — receive (раздел 2.1/2.2).

> 🎯 **Итоговый фокус оптимизации — WS receive hot path** (разделы 2.1/2.2): убирает `CancellationTokenSource.Register` (~17–19%, вызываемый на каждое ~21K-е сообщение/с), что устраняет самый дорогой и contention-образующий источник.
>
> Альтернатива для MDP (если потребуется позже, вне текущего scope): fast-path `if (channel.Reader.Count == 0 && batchCount == 0) → WaitToReadAsync(cancellationToken)` + отдельный таймерный флаш partial batch через примитив без `CancelAfter` (напр. `Channel`/`Semaphore` + `Task.Delay`), без гонок `TryReset`.

---

## 3. Критерии успеха (подтвердить нагрузочным прогоном)

| Метрика | Baseline 045558 | Ожидание |
|---------|:---:|:---:|
| `CancellationTokenSource.*` суммарно в topN | ~40% | **значительно ниже** (перестали быть #1) |
| `CancellationTokenSource.Register` | 17.37% | **~0%** из receive hot path |
| Lock contention | 2,392 | **существенно ниже** |
| Аллокации на тик | ~619 Б | **ниже** |
| Дропы | 0 | **0 (не регрессировать)** |
| Backlog канала | ≤1 | без регрессии |

---

## 4. Верификация без нагрузки

- Сборка всех модулей;
- `dotnet format` (правило проекта);
- Полный прогон юнит-тестов (`run_test.ps1` или эквивалент):
  - `WebSocketMessageReceiverTests`, `WebSocketConnectionManagerTests` (моки `ReceiveAsync(…, It.IsAny<CancellationToken>())` — токен меняется на `None`, тесты проверяют факт вызова, а не значение токена);
  - `MarketDataProcessorTests` (flush-логика, канал закрывается корректно).

## 5. Нагрузочный прогон (по согласованию с пользователем)

- `run_all_profiler.ps1` (режим `all`, FakeTickServer);
- сравнение `lock_contention`, `topN`-доли `CancellationTokenSource.*`, байт/тик с baseline `045558`.

---

## 6. Риски

| Риск | Оценка | Митигация |
|------|:---:|---|
| `CancellationToken.None` в receive → зависание при остановке | Средняя | Остановка всё равно идёт через `StopReceiveLoopAsync` (отмена `_loopCts` + ожидание завершения); loop выйдёт по `IsCancellationRequested` на верхней проверке. Подтверждено тестом `StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop` (PASS). |
| `TryReset` гонки при timer flush | Высокая (реализовано, откат) | `/!\ ПОДТВЕРЖДЕНО: TryReset не переводит сгоревший CTS в активное → бесконечный цикл при StopProcessingAsync. MDP НЕ трогаем.` |
| Двойной флаш / потеря partial batch при отмене | Низкая | Логика `batchCount > 0` и обработка `OperationCanceledException` сохраняются без изменений. |

---

## 7. Файлы

| Файл | Действие |
|------|----------|
| [`WebSocketConnectionManager.cs`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs) | ✔ receive → `CancellationToken.None` |
| [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs) | ✔ оба receive → `CancellationToken.None` |
| [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) | ✔ (откачено — без изменений) |
| `tests/…/WebSocketMessageReceiverTests.cs` | ✔ без изменений (моки `It.IsAny<CancellationToken>()` совместимы) |
| `tests/…/WebSocketConnectionManagerTests.cs` | ✔ без изменений (verify на `CancellationToken.None` совпадает) |
| `tests/…/MarketDataProcessorTests.cs` | ✔ без изменений (откат) |

---

## 8. Итог реализации

- **Принято и реализовано:** `CancellationToken.None` в WS receive hot path (аналогично уже принятому в `execution-plan-ws-cts-async.md` и отдельно — `WebSocketConnectionManager.ReceiveAsync`). Убирает регистрацию отмены на каждое сообщение → снимает главный `Register` ~17–19% + contention.
- **Откачено:** переиспользуемый flush-CTS в `MarketDataProcessor` (флакю-нестабилен из-за `TryReset`, см. §2.3).
- **Тесты:** WS-специфичные и MDP-тесты прошли в первом полном прогоне (кроме `FlushTimer_FlushesPartialBatch_OnTimerTick`, который завис именно из-за откаченной MDP-правки). После отката требуется чистый повторный прогон.
- **Нагрузочная верификация — следующий шаг** (по согласованию): `run_all_profiler.ps1` с сравнением topN vs 045558.