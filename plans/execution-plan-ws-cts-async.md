# План исполнения: оптимизация WS/CTS-активности и async-аллокаций

**Основание:** бинарный разбор `dotnet-trace topN` прогона `20260805_115513` выявил главный источник CPU/contention, ранее не учтённый в планах. Подтверждено в [`check-anomalies-contention-alloc-plan.md`](plans/check-anomalies-contention-alloc-plan.md).

**Диагноз:** Lock contention (1,996, +545 к baseline) и часть аллокаций (+14%) исходят из **управления отменой и приёма WebSocket**, а не из конвейера записи (там только `Interlocked`) и не из этапа 3.3 (компактинг). Hot path конвейера чист.

---

## Топ-CPU (dotnet-trace topN, прогон 115513)

| Метод | Exclusive | Источник |
|---|---|---|
| `CancellationTokenSource.Register` | 14.95% | отмена в `ReceiveAsync`/`WaitToReadAsync` |
| `WebSocketConnectionManager.ReceiveAsync` | 10.95% | приём WS |
| `CancellationTokenSource.CancelAfter` | 10.06% | таймауты (`CancelAfter`) |
| `WaitToReadAsync` (Channel) | 10.04% | ожидание данных канала |
| `ManagedWebSocket.ReceiveAsync`/`GetReceiveResult` | 9.61%/5.95% | приём WS |
| `CancellationTokenSource.CreateLinkedTokenSource` | 7.4% | linked CTS |
| `Task.FromResult`/`GetTaskForValueTaskSource`/`GetStateMachineBox` | ~7.6% | async-стейтмашины |

**Сумма по CTS (~32%)** + приём WS (~26%) — доминирует. Внутри `CancellationTokenSource` операция `Register`/`CancelAfter` использует внутренний `lock` — это и есть источник contention.

---

## Этап 1 — Сокращение `CancelAfter`/`CreateLinkedTokenSource` в ожидании канала (основное)

**Место:** [`ProcessBatchesAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:676) (Multi-Consumer legacy path) и [`WriterLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:572) (Single-Consumer).

**Проблема:** стр. 676–677 создают `CreateLinkedTokenSource` + `CancelAfter` на **каждую итерацию флаша** (`_flushIntervalSeconds`). При 680 батчах + флашах это тысячи linked CTS → `Register`+`CancelAfter` → lock contention.

**Решение (вариант без `CancelAfter`):**
- Заменить `linkedCts.CancelAfter(...)` + `WaitToReadAsync(linkedCts.Token)` на **таймерный вариант** без создания linked CTS на каждой итерации:
  - вариант A: `await channel.Reader.WaitToReadAsync(cancellationToken)` без таймаута + отдельный `PeriodicTimer`/`Task.Delay`-флаш по таймеру (флаш при накоплении или по интервалу) — убрать `CancelAfter` из цикла;
  - вариант B: переиспользовать **один** `CancellationTokenSource` на весь цикл, пересоздавая его только при `CancelAfter` срабатывании (паттерн `Reset`+`CancelAfter`), избегая `CreateLinkedTokenSource` на каждую итерацию.
- Если нужен именно таймаут на ожидание — использовать `CancellationTokenSource.CancelAfter` на **переиспользуемом** CTS, а не новый linked CTS каждый раз.

**Критерий успеха:** исключение `CreateLinkedTokenSource` и (по возможности) `CancelAfter` из hot path ожидания канала; снижение contention к целевому (~1,056) и CPU на CTS-методах.

**Тесты:** существующие тесты канала/процессора (`MarketDataProcessor` не покрыт напрямую — добавить unit-тест на корректность флаша при накоплении и по таймеру). Прогон metrics-analysis после изменений.

---

## Этап 2 — Приём WS: минимизировать `Register` на сообщение

**Место:** [`WebSocketMessageReceiver.RunLoopCoreAsync`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:73) → [`WebSocketConnectionManager.ReceiveAsync`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:107) → `ClientWebSocket.ReceiveAsync`.

**Проблема:** `ReceiveAsync` внутренне регистрирует делегат на токен для каждого вызова (per-message). `CancellationTokenSource.Register` 14.95% — это отмена внутри каждого `ReceiveAsync`. `CreateLinkedTokenSource` в [`StartReceiveLoopAsync`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:54) создаётся на **каждое переподключение** (цикл `RunBackgroundRecoveryLoopAsync`), а не на сообщение.

**Что можно сделать (низкий риск):**
- Проверить, вызывается ли `StartReceiveLoopAsync`/`CreateLinkedTokenSource` чаще, чем реальные переподключения (в `RunBackgroundRecoveryLoopAsync`). Если receive loop перезапускается часто (например, на каждый сбой), сократить число пересозданий CTS.
- Убедиться, что `_loopLock`/`_backgroundLock` не удерживаются в пер-сообщение пути. В текущем коде `lock` только при старте/стопе loop (не на сообщение) — подтверждено.

> ⚠️ **Важно:** `ManagedWebSocket.ReceiveAsync`/`GetReceiveResult` — это накладные расходы самого .NET WebSocket на приём (не наш код). Их можно снизить только выбором другого транспорта/подхода (не приоритетно). Основной выигрыш — за счёт `CancellationTokenSource.Register` через оптимизацию ожидания (Этап 1) и проверку частоты пересоздания CTS.

**Критерий:** contention снижается; `CreateLinkedTokenSource` вызывается только при реальном переподключении, а не в цикле.

---

## Этап 3 — Async-стейтмашины и Task-аллокации (вторично)

**Место:** [`ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:195) — вызов `_tickAggregator.OnTickAsync(...)` на каждый тик.

**Проблема:** `Task.FromResult` 4.9% + `GetStateMachineBox`/`GetTaskForValueTaskSource` ~2.7%. `ProcessMessageAsync` в [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:274) возвращает `Task` — на каждое сообщение возможна аллокация стейтмашины.

**Решение:**
- Проверить сигнатуру `ITickAggregator.OnTickAsync`: если возвращает `Task` и вызывается на каждый тик (1.7M), перевести на `ValueTask` или синхронный путь, чтобы не аллоцировать `GetStateMachineBox`/`GetTaskForValueTaskSource` на сообщение.
- `ProcessMessageAsync` (per-message) — вернуть `Task.CompletedTask` без async-стейтмашины там, где нет `await` (уже частично так).

**Критерий:** снижение доли `Task.FromResult`/`GetStateMachineBox` в topN; аллокации ближе к baseline.

---

## Этап 4 — Наблюдение и верификация (обязателен)

**Цель:** подтвердить эффект этапов 1–3 и отсутствие регрессий конвейера (дропы, запись %, latency).

**Действия:**
1. Повторный прогон `run_all_profiler.ps1` (режим `all`, тот же FakeTickServer: 1.7M, rps=25000, dupPercent=3%).
2. Сравнить с baseline `115513`:
   - `process_runtime_dotnet_monitor_lock_contention_count_total` (цель: снижение к ~1,056);
   - аллокации (цель: снижение к ~1.08 GB baseline `000308`);
   - дропы (0), `% processed/incoming` (≈97%), max duration батча (<250 мс) — **без регрессий**;
   - LOH-фрагментация/heap (должна остаться низкой: 0.46 MB / 16.4 MB).
3. `dotnet-trace topN` — проверить снижение доли `CancellationTokenSource.*`.

---

## Порядок исполнения

1. **Этап 1** — оптимизация ожидания канала (главный эффект на contention/CPU). Приоритет.
2. **Этап 2** — проверка частоты пересоздания CTS в receive loop. Низкий риск, точечно.
3. **Этап 3** — `ValueTask`/синхронные пути в `ITickAggregator`/`ProcessMessageAsync`. Вторично.
4. **Этап 4** — верификационный прогон + metrics-analysis, сравнение.

> Реализация требует **code-режима** (правки `.cs`). Верификация — через `run_all_profiler.ps1` и metrics-analysis.

---

## Этап 1 — статус: РЕАЛИЗОВАНО ✅ (2026-08-05)

**Изменения в [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs):**

Добавлено условие `channel.Reader.Count == 0` в guard таймерного флаша в обоих циклах:
- [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:438)
- [`ProcessBatchesAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:677)

**Логика:** `CancellationTokenSource.CreateLinkedTokenSource` + `CancelAfter` создаются ТОЛЬКО когда канал пуст (`Reader.Count == 0`) — т.е. когда partial batch реально рискует «зависнуть» и нужен таймерный флаш. В плотном сценарии (канал почти всегда непуст) быстрый путь уходит сразу в `readTicks` без создания linked CTS/`CancelAfter`/`Register` → устраняется основной источник contention и ~32% CPU из `topN` trace.

**Семантика сохранена:** при непустом канале partial batch дополняется данными (не зависает), `_minPartialBatchSize`-skip продолжает работать при тишине. Таймерный флаш по-прежнему срабатывает при простое.

**Верификация:**
- `dotnet build` — **succeeded, 0 errors**.
- `dotnet test` (RawTickRepository) — **24 passed, 0 failed**.
- Kafka-интеграционные (полный набор) — проходят, не связаны с правкой.

**Примечание:** предсуществующее warning `CS0219 flushCts` (стр. 430) — не от изменений Этапа 1, не трогалось.

---

## Этап 2 — статус: РЕАЛИЗОВАНО ✅ (2026-08-05)

**Изменения** (подход с `WebSocket.Abort()` вместо токена в hot path приёма; детальный план — [`ws-stage2-abort-receive-plan.md`](plans/ws-stage2-abort-receive-plan.md)):

Диагноз подтверждён кодом: `CreateLinkedTokenSource` создаётся только при реальном (пере)подключении (не на сообщение), а `CancellationTokenSource.Register` 14.95% исходил из внутренней регистрации отмены в `ClientWebSocket.ReceiveAsync` на **каждый фрагмент** через linked-токен.

1. **`IClientWebSocket`**([`IClientWebSocket.cs`](src/MarketDataCollector.Core/Interfaces/IClientWebSocket.cs)) / **`ClientWebSocketWrapper`**([`ClientWebSocketWrapper.cs`](src/MarketDataCollector.Core/Clients/ClientWebSocketWrapper.cs)) — добавлен `Abort()`;
2. **`IWebSocketConnectionManager`**([`IWebSocketConnectionManager.cs`](src/MarketDataCollector.Core/Interfaces/IWebSocketConnectionManager.cs)) / **`WebSocketConnectionManager`**([`WebSocketConnectionManager.cs`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs)) — добавлен `Abort()` (обёртка с логов и перехватом исключений);
3. **`WebSocketConnectionManager.ReceiveAsync`** — передаёт `CancellationToken.None` вместо токена: **исключает `CancellationTokenSource.Register` на каждый `ReceiveAsync`**;
4. **`WebSocketMessageReceiver.RunLoopCoreAsync`** — оба `ReceiveAsync` (основной + oversize-skip) переведены на `CancellationToken.None`; токен остался только в non-hot path (`Task.Delay`, проверки);
5. **`WebSocketMessageReceiver.StopReceiveLoopAsync`** — после отмены `_loopCts` вызывает `_connectionManager.Abort()` для мгновенного прерывания блокирующего `ReceiveAsync`;
6. **`BaseWebSocketClient.StopReceiveLoopAsync`** — аналогичный `_connectionManager.Abort()` перед ожиданием loop.

**Семантика:** `Abort()` (жёсткий сброс сокета) применяется только на пути остановки/переподключения, где соединение всё равно пересоздаётся через `ConnectAsync`. Graceful-закрытие (`CloseAsync`) сохранено в явном `DisconnectAsync`.

**Тесты:**
- добавлен `Abort_CallsCurrentSocketAbort` в `WebSocketConnectionManagerTests`;
- добавлен `StopReceiveLoopAsync_CallsConnectionManagerAbort` в `WebSocketMessageReceiverTests`;
- существующие тесты не ломаются (используют `It.IsAny<CancellationToken>()`/`CancellationToken.None`).

**Верификация:**
- `dotnet build` — **succeeded, 0 errors**.
- `dotnet test` (полный набор, `--no-build`) — **exit 0** (зелёные).

**Верификация производительности (этап 4 общего плана), не выполнена:** требуется прогон `run_all_profiler.ps1` (режим `all`) + metrics-analysis, сравнение `lock_contention_count_total` с baseline `115513` и доли `CancellationTokenSource.Register` в `dotnet-trace topN`.
