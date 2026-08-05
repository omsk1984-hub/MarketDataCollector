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
