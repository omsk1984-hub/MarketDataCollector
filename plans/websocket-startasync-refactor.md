# Рефакторинг: Вынос StartAsync/StopAsync в интерфейс

## Проблема

В `Worker.cs:95-97` есть type-check:
```csharp
if (client is BaseWebSocketClient baseClient)
    await baseClient.StartAsync(stoppingToken);
else
    await client.ConnectAsync(stoppingToken);
```

Это нарушает полиморфизм — Worker знает о конкретном типе.

## Решение

Добавить `StartAsync`/`StopAsync` в `IExchangeWebSocketClient`.

## Шаги

### 1. Добавить методы в IExchangeWebSocketClient

```csharp
public interface IExchangeWebSocketClient : IDisposable
{
    // ... существующие члены ...
    
    /// <summary>
    /// Запускает клиент с автоматическим восстановлением соединения.
    /// </summary>
    Task StartAsync(CancellationToken ct);
    
    /// <summary>
    /// Останавливает клиент и фоновое восстановление.
    /// </summary>
    Task StopAsync(CancellationToken ct);
}
```

**Файл:** `src/MarketDataCollector.Core/Interfaces/IExchangeWebSocketClient.cs`

### 2. BaseWebSocketClient уже реализует эти методы

`BaseWebSocketClient` уже имеет `public virtual Task StartAsync(CancellationToken)` и `public virtual Task StopAsync(CancellationToken)`. 

Нужно просто добавить явную реализацию интерфейса (если сигнатуры совпадают — ничего не меняем).

**Файл:** `src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs`

### 3. Обновить Worker.cs

**Было:**
```csharp
if (client is BaseWebSocketClient baseClient)
{
    await baseClient.StartAsync(stoppingToken);
}
else
{
    await client.ConnectAsync(stoppingToken);
}
```

**Стало:**
```csharp
await client.StartAsync(stoppingToken);
```

Аналогично для `StopClientAsync`:
```csharp
// Было:
if (client is BaseWebSocketClient baseClient)
    await baseClient.StopAsync(CancellationToken.None);
else
{
    if (client.IsConnected)
        await client.DisconnectAsync(CancellationToken.None);
}

// Стало:
await client.StopAsync(CancellationToken.None);
```

**Файл:** `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs`

### 4. Проверить другие реализации

- `BinanceWebSocketClient` — наследует `BaseWebSocketClient`, не переопределяет `StartAsync`/`StopAsync` → OK
- Другие клиенты — проверить, что все реализуют `IExchangeWebSocketClient`

### 5. Проверить компиляцию

```bash
dotnet build
```

## Диаграмма

### До
```
Worker ──type-check──> BaseWebSocketClient.StartAsync
     └──fallback──> IExchangeWebSocketClient.ConnectAsync
```

### После
```
Worker ──> IExchangeWebSocketClient.StartAsync
              ↑
    BaseWebSocketClient (реализация)
              ↑
    BinanceWebSocketClient (наследует)
```

## Влияние на код

| Файл | Изменение |
|------|-----------|
| `IExchangeWebSocketClient.cs` | +2 метода |
| `BaseWebSocketClient.cs` | Без изменений (уже реализует) |
| `Worker.cs` | -type-check, +прямой вызов |
| `BinanceWebSocketClient.cs` | Без изменений |

## Плюсы

- Убрана зависимость от конкретного типа в Worker
- Полиморфизм — любой клиент может реализовать автовосстановление
- Единообразие — все методы жизненного цикла через интерфейс
