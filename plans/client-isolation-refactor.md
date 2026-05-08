# План: Изоляция перезапуска WebSocket-клиентов

## Проблема

Текущая архитектура имеет следующие проблемы с изоляцией:

1. **Падение `MarketDataProcessor` убивает всех клиентов.** Если `StartProcessing()` или `ProcessBatchesAsync()` выбрасывает исключение — срабатывает `catch` в `Worker`, все клиенты останавливаются и перезапускаются вместе.

2. **Упавший клиент не восстанавливается самостоятельно.** Если `RunBackgroundRecoveryLoopAsync` исчерпает `_maxReconnectAttempts` и завершится — `Worker` об этом не узнает. Health-check только логирует, но не перезапускает клиент.

3. **Нет индивидуального контроля жизненного цикла.** Все клиенты запускаются и останавливаются как единое целое через `Task.WhenAll`.

## Цель

Каждый WebSocket-клиент должен иметь **полностью независимый жизненный цикл**:
- Падение одного клиента не влияет на остальных
- Клиент перезапускается автоматически без участия `Worker`
- `Worker` только отслеживает состояние и логирует

## Архитектура решения

### Текущая схема

```
Worker.RunWithRecoveryAsync
  ├── Task.WhenAll(clients.StartAsync)     ← запуск всех вместе
  ├── marketDataProcessor.StartProcessing()
  ├── Task.Delay(Timeout.Infinite)          ← блокировка
  └── catch → CleanupAsync(ALL) → retry     ← перезапуск ВСЕХ
```

### Целевая схема

```
Worker.RunWithRecoveryAsync
  ├── marketDataProcessor.StartProcessing()
  ├── clients.ForEach(c => c.StartAsync())  ← каждый сам за себя
  ├── HealthCheck loop (мониторинг)
  └── catch → только при падении Processor
      └── CleanupAsync(ALL) → retry

Client.RunBackgroundRecoveryLoopAsync      ← бесконечный цикл
  ├── ConnectAsync (с retry)
  ├── Мониторинг соединения
  └── При разрыве → снова ConnectAsync     ← самовосстановление
```

## Ключевые изменения

### 1. `BaseWebSocketClient` — бесконечный цикл восстановления

**Проблема:** `RunBackgroundRecoveryLoopAsync` завершается после `_maxReconnectAttempts = 10`.

**Решение:** Убрать ограничение на количество попыток. Цикл должен быть бесконечным до явной отмены через `StopAsync`.

```csharp
// БЫЛО: выход после 10 попыток
if (reconnectAttempt >= _maxReconnectAttempts)
{
    await LogAsync("Превышено максимальное количество попыток переподключения");
    break;  // ← клиент умирает навсегда
}

// СТАНЕТ: бесконечные попытки с прогрессивной задержкой
reconnectAttempt++;
var delay = CalculateBackoff(reconnectAttempt);  // экспоненциальный backoff с cap
await Task.Delay(delay, cancellationToken);
// цикл продолжается
```

### 2. `Worker` — убрать зависимость от падения клиентов

**Проблема:** `try/catch` в `Worker` ловит ВСЕ исключения и перезапускает ВСЁ.

**Решение:** `Worker` должен перезапускать клиентов только при падении `MarketDataProcessor`. Клиенты сами управляют своим жизненным циклом.

```csharp
// БЫЛО: любое исключение → перезапуск всех
catch (Exception ex)
{
    _logger.LogError(ex, "Error occurred. Retrying...");
    // Cleanup всех клиентов → перезапуск всех
}

// СТАНЕТ: разделение ответственности
catch (Exception ex) when (ex is not OperationCanceledException)
{
    // Только если упал Processor или другой критический компонент
    _logger.LogError(ex, "Critical error. Restarting all...");
    // Cleanup → перезапуск
}
```

### 3. `Worker` — Health-check с активным мониторингом

**Проблема:** Health-check только логирует статус.

**Решение:** Health-check проверяет `IsConnected` и при необходимости явно вызывает `StartAsync` для отключённых клиентов (на случай если фоновая задака каким-то образом завершилась).

```csharp
private async Task RunHealthCheckAsync(
    List<IExchangeWebSocketClient> clients,
    CancellationToken stoppingToken)
{
    using var timer = new Timer(async _ =>
    {
        foreach (var client in clients)
        {
            if (!client.IsConnected)
            {
                _logger.LogWarning("{Exchange} disconnected, triggering restart...",
                    client.ExchangeName);
                // StartAsync идемпотентен — безопасно вызывать повторно
                await client.StartAsync(stoppingToken);
            }
        }
    }, null, HealthCheckInterval, HealthCheckInterval);

    await Task.Delay(Timeout.Infinite, stoppingToken);
}
```

### 4. `IExchangeWebSocketClient` — добавить событие `Faulted`

Для случаев, когда клиент всё-таки не может восстановиться (например, отмена через `CancellationToken`):

```csharp
public interface IExchangeWebSocketClient : IDisposable
{
    // ... существующие члены ...

    /// <summary>
    /// Срабатывает, когда клиент окончательно прекращает работу
    /// (отмена через CancellationToken, не восстанавливаемая ошибка).
    /// </summary>
    event EventHandler<string> Faulted;
}
```

## Диаграмма жизненного цикла

```
┌─────────────────────────────────────────────────────────────┐
│                        Worker                               │
│                                                             │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │
│  │  Client A    │   │  Client B    │   │  Client C    │    │
│  │  (Binance)   │   │  (Kraken)    │   │  (Bybit)     │    │
│  │              │   │              │   │              │    │
│  │ ┌──────────┐ │   │ ┌──────────┐ │   │ ┌──────────┐ │    │
│  │ │Recovery  │ │   │ │Recovery  │ │   │ │Recovery  │ │    │
│  │ │Loop      │ │   │ │Loop      │ │   │ │Loop      │ │    │
│  │ │(own Task)│ │   │ │(own Task)│ │   │ │(own Task)│ │    │
│  │ └──────────┘ │   │ └──────────┘ │   │ └──────────┘ │    │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘    │
│         │                  │                  │            │
│         └──────────┬───────┴──────────────────┘            │
│                    │                                        │
│            ┌───────▼───────┐                                │
│            │  HealthCheck  │  (мониторинг + перезапуск)     │
│            └───────┬───────┘                                │
│                    │                                        │
│            ┌───────▼───────┐                                │
│            │  Processor    │                                │
│            └───────────────┘                                │
└─────────────────────────────────────────────────────────────┘

Падение Client B → перезапускается только B, A и C продолжают работу
Падение Processor → перезапуск всех (обоснованно)
```

## Шаги реализации

### Шаг 1: Изменить `RunBackgroundRecoveryLoopAsync` в `BaseWebSocketClient`

- Убрать `break` после исчерпания `_maxReconnectAttempts`
- Заменить на бесконечный цикл с экспоненциальным backoff (с  cap, например, 60 сек)
- Единственный способ выхода — `cancellationToken.IsCancellationRequested`

### Шаг 2: Сделать `StartAsync` идемпотентным

- Гарантировать, что повторный вызов `StartAsync` не создаёт дублирующих фоновых задач
- Текущая реализация уже содержит эту проверку (строка 96), но нужно убедиться в потокобезопасности

### Шаг 3: Рефакторить `Worker.RunWithRecoveryAsync`

- Разделить обработку ошибок: падение Processor vs падение клиента
- Убрать `try/catch` вокруг `Task.WhenAll(startTasks)` (он бессмысленен, т.к. `StartAsync` возвращает `Task.CompletedTask`)
- Добавить отдельный Health-check с активным мониторингом

### Шаг 4: Обновить `CleanupAsync`

- Оставить как есть — корректно останавливает всех клиентов
- Вызывается только при критических ошибках (падение Processor) или при остановке Worker

### Шаг 5: Проверить компиляцию и запуск

```bash
dotnet build
dotnet run --project src/MarketDataCollector.Workers/MarketDataCollector.Worker
```

## Риски и ограничения

| Риск | Митигация |
|------|-----------|
| Утечка задач при повторном `StartAsync` | Проверка `_backgroundRecoveryTask.IsCompleted` в `StartAsync` |
| ЭкспоненENTIAL backoff без cap → огромные задержки | Добавить `Math.Min(delay, maxDelay)` с cap = 60 сек |
| `StopAsync` не завершает задачу мгновенно | `CancellationTokenSource.Cancel()` + `WaitAsync` с таймаутом |
| Потокобезопасность `IsConnected` | Уже используется `Volatile.Read` — OK |

## Файлы для изменения

| Файл | Изменение |
|------|-----------|
| `src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs` | Бесконечный цикл восстановления, убрать `_maxReconnectAttempts` |
| `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs` | Разделение обработки ошибок, активный Health-check |
| `src/MarketDataCollector.Core/Interfaces/IExchangeWebSocketClient.cs` | (опционально) Добавить событие `Faulted` |
