# P1: Устранить `Task.WhenAny` в CollectorLoop — план реализации

**Дата:** 05.08.2026  
**Основание:** [`optimization-verification-report.md`](plans/optimization-verification-report.md) — раздел 4: `Task.WhenAny` + `Task.Delay` дают ~23% CPU (`WhenAny` 12.38% + `Task.Delay` 10.97%).

---

## 1. Проблема

В двух циклах сбора/обработки батчей используется паттерн **timer-flush** через `Task.WhenAny`:

```csharp
var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
flushTimerCts.TryReset();
flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);
```

**Почему это дорого:**
- `Task.WhenAny` аллоцирует `Task[]` на 2 элемента + continuations на каждый вызов
- `Task.Delay(Timeout.Infinite, cts.Token)` аллоцирует `Task` + `CancellationToken` регистрацию
- При ~7.5 батча/с (680 батчей / 90 с) это ~15 вызовов `Task.WhenAny` в секунду

**Места:**
1. [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:457) — строка 457, **основной путь**
2. [`ProcessBatchesAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:701) — строка 701, **legacy multi-consumer**

---

## 2. Решение

Заменить паттерн на `WaitToReadAsync(CancellationToken)` с **ручным таймером через `CancellationTokenSource.CancelAfter()`**.

### Было:
```csharp
var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
flushTimerCts.TryReset();
flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);
// ...
if (completed == flushDelay) { /* flush */ }
if (readTaskTyped.Result) { /* read */ }
```

### Стало:
```csharp
using var flushCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
flushCts.CancelAfter(TimeSpan.FromSeconds(_flushIntervalSeconds));
try
{
    var hasData = await channel.Reader.WaitToReadAsync(flushCts.Token).ConfigureAwait(false);
    if (hasData) { /* readTicks */ }
    else { /* channelCompleted */ }
}
catch (OperationCanceledException) when (flushCts.IsCancellationRequested)
{
    // Таймаут — отправляем частичный батч
}
```

**Ключевые изменения:**
- Убирается `Timer` — больше не нужен, `CancelAfter` делает то же самое
- Убирается `Task.Delay(Timeout.Infinite, cts.Token)` — не создаётся бесконечная задача
- Убирается `Task.WhenAny(readTask, flushDelay)` — не аллоцируется массив задач
- Используется `CancellationTokenSource.CreateLinkedTokenSource` для связки с внешним токеном отмены
- Упрощается код: уходит `goto` (или его объём уменьшается)

---

## 3. Оценка эффекта

| Метрика | До | После | Экономия |
|---------|:--:|:-----:|:--------:|
| `Task.WhenAny` CPU | 12.38% | 0% | **−12.38%** |
| `Task.Delay` CPU | 10.97% | 0% | **−10.97%** |
| Аллокации/батч (Task[]) | ~120 байт | 0 | **~120 байт/батч** |
| Всего аллокаций за прогон | ~81.6 KB | 0 | **~81.6 KB** |
| **Итого CPU** | **23.35%** | **0%** | **−23.35%** |

---

## 4. План реализации

### Шаг 1. Изменить `CollectorLoopAsync` (строки 430-538)

- Удалить `Timer? flushTimer` и его инициализацию
- Заменить блок `if (_flushIntervalSeconds > 0 && batchCount > 0)` (строки 451-504) на новую логику с `CancelAfter`
- Сохранить всю логику: flush по таймауту, чтение тиков, отправка в батч-канал
- Убрать `flushTimer.Dispose()` из finally (если есть)
- `flushTimerCts` (CancellationTokenSource) — заменить на `flushCts` с `CancelAfter`

### Шаг 2. Изменить `ProcessBatchesAsync` (строки 680-734)

Аналогичные изменения для legacy multi-consumer пути.

### Шаг 3. Сборка и прогон тестов

```powershell
dotnet build src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj
dotnet test tests/MarketDataCollector.Tests/MarketDataCollector.Tests.csproj
```

### Шаг 4. Прогон нагрузочного теста (после утверждения)

```powershell
.\start_loadtest.ps1
```

---

## 5. Риски

| Риск | Вероятность | Митигация |
|------|:-----------:|-----------|
| `CancelAfter` сработал, но мы уже начали читать тики | Низкая | `WaitToReadAsync` атомарен — либо получает данные, либо бросает `OperationCanceledException` |
| Linked CTS не освобождён (утечка регистрации) | Средняя | Использовать `using` для `CreateLinkedTokenSource` |
| Изменение тайминга влияет на adaptive batch size | Низкая | Логика `flushInterval` не меняется — только механизм реализации |
| `TryReset` может не успеть перед повторным `CancelAfter` | Низкая | `CancelAfter` можно вызывать после `TryReset` — CTS переходит в неактивное состояние после срабатывания |