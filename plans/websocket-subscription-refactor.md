# План рефакторинга: Автоматическая подписка при реконнекте

## Проблема

При автоматическом переподключении WebSocket соединения **подписка не повторяется**, что приводит к прекращению получения данных.

### Текущее поведение

```
Worker.StartClientWithRecoveryAsync()
  ├── client.StartAsync()          // Запускает фоновый цикл восстановления
  │     └── RunBackgroundRecoveryLoopAsync()
  │           └── ConnectAsync()   // Подключение к WebSocket
  │                 (подписка НЕ вызывается!)
  │
  └── SubscribeWithRetryAsync()    // Подписка только при старте
        └── client.SubscribeToTicker()
```

### Результат

- **Первый старт**: Работает ✓
- **После реконнекта**: Соединение есть, но данные не приходят ✗

---

## Варианты решения

### Вариант 1: Подписка в URL (рекомендуемый для Binance)

**Идея**: Встроить подписку прямо в URL соединения, как в тестовом `BinanceTickMonitor`.

```
Было:  wss://stream.binance.com:9443/ws
Стало: wss://stream.binance.com:9443/ws/btcusdt@trade
```

**Плюсы:**
- Минимальные изменения в коде
- Подписка автоматически при каждом подключении
- Работает с текущей архитектурой реконнекта

**Минусы:**
- Работает только для бирж, поддерживающих этот формат (Binance ✓, Kraken ✗)
- Нужна разная логика для разных бирж

---

### Вариант 2: Подписка в ConnectAsync

**Идея**: Вызывать `SubscribeToTicker` после каждого успешного подключения в `BaseWebSocketClient.ConnectAsync`.

```csharp
public virtual async Task ConnectAsync(CancellationToken cancellationToken)
{
    await _retryPolicy.ExecuteAsync(async () =>
    {
        // ... подключение ...
        
        OnConnected();
        await StartReceiveLoopAsync();
        await SubscribeToTickerAsync();  // <-- Добавить здесь
    });
}
```

**Плюсы:**
- Работает для всех бирж
- Подписка автоматически при каждом реконнекте
- Минимальные изменения

**Минусы:**
- Нужно добавить абстрактный метод `SubscribeToTickerAsync` в `BaseWebSocketClient`
- Изменяет контракт `ConnectAsync`

---

### Вариант 3: Событие Connected + Worker

**Идея**: Worker подписывается на событие `Connected` и вызывает `SubscribeToTicker`.

```csharp
// В Worker.cs
client.ClientConnected += async (s, e) => 
{
    await SubscribeWithRetryAsync(client, stoppingToken);
};
```

**Плюсы:**
- Разделение ответственности
- Не изменяет `BaseWebSocketClient`

**Минусы:**
- Более сложная логика
- Нужно добавить событие `ClientConnected` в `IExchangeWebSocketClient`
- Возможны race condition

---

## Рекомендация

**Для Binance**: Вариант 1 (подписка в URL) — самый простой и надёжный.

**Для мультибиржевой поддержки**: Вариант 2 (подписка в `ConnectAsync`) — более универсальный.

---

## План реализации (Вариант 2)

### Шаг 1: Добавить абстрактный метод в BaseWebSocketClient

```csharp
// BaseWebSocketClient.cs
protected internal virtual Task SubscribeToTickerAsync(CancellationToken cancellationToken)
{
    // По умолчанию ничего не делает (для бирж с подпиской в URL)
    return Task.CompletedTask;
}
```

### Шаг 2: Вызвать подписку в ConnectAsync

```csharp
// BaseWebSocketClient.cs, метод ConnectAsync
await _retryPolicy.ExecuteAsync(async () =>
{
    // ... существующий код подключения ...
    
    OnConnected();
    await StartReceiveLoopAsync();
    await SubscribeToTickerAsync(cancellationToken);  // Добавить
});
```

### Шаг 3: Переопределить в BinanceWebSocketClient

```csharp
// BinanceWebSocketClient.cs
protected internal override async Task SubscribeToTickerAsync(CancellationToken cancellationToken)
{
    var subscribeMessage = $"{{\"method\":\"SUBSCRIBE\",\"params\":[\"{Symbol.ToLower()}@trade\"],\"id\":1}}";
    await SendAsync(subscribeMessage, cancellationToken);
}
```

### Шаг 4: Упростить Worker.cs

```csharp
// Worker.cs - убрать отдельный вызов SubscribeWithRetryAsync
private async Task StartClientWithRecoveryAsync(IExchangeWebSocketClient client, CancellationToken stoppingToken)
{
    try
    {
        await client.StartAsync(stoppingToken);
        _logger.LogInformation("Started {Exchange} ({Symbol})", client.ExchangeName, client.Symbol);
    }
    catch (Exception ex)
    {
        _logger.LogError(ex, "Failed to start client {Exchange} ({Symbol})", 
            client.ExchangeName, client.Symbol);
    }
}
```

### Шаг 5: Добавить retry для подписки в BaseWebSocketClient

```csharp
// BaseWebSocketClient.cs
private async Task SubscribeToTickerWithRetryAsync(CancellationToken cancellationToken)
{
    var retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(
            retryCount: 3,
            sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            onRetry: (exception, timeSpan, retryCount, context) =>
            {
                OnErrorOccurred(new Exception($"Subscribe attempt {retryCount} failed", exception));
            });

    await retryPolicy.ExecuteAsync(async () =>
    {
        await SubscribeToTickerAsync(cancellationToken);
    });
}
```

---

## Диаграмма нового потока

```
Worker.StartClientWithRecoveryAsync()
  └── client.StartAsync()
        └── RunBackgroundRecoveryLoopAsync()
              └── ConnectAsync()  [при каждом реконнекте]
                    ├── WebSocket.ConnectAsync()
                    ├── OnConnected()
                    ├── StartReceiveLoopAsync()
                    └── SubscribeToTickerAsync()  ← Автоматически!
                          └── SubscribeToTickerWithRetryAsync()
```

---

## Файлы для изменения

| Файл | Изменения |
|------|-----------|
| `BaseWebSocketClient.cs` | Добавить `SubscribeToTickerAsync`, вызвать в `ConnectAsync` |
| `BinanceWebSocketClient.cs` | Переопределить `SubscribeToTickerAsync` |
| `Worker.cs` | Убрать `SubscribeWithRetryAsync`, упростить `StartClientWithRecoveryAsync` |

---

## Вопросы для уточнения

1. Какие биржи планируется поддерживать? (влияет на выбор варианта)
2. Нужна ли поддержка нескольких символов на одного клиента?
3. Есть ли биржи, которые не требуют подписки (данные приходят автоматически)?
