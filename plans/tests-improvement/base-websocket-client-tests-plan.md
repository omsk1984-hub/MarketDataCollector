# План улучшения: BaseWebSocketClientTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `SetSubscriptionManager_SetsManagerCorrectly` — неверная проверка

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:222)

**Описание:** Тест вызывает `client.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult()`, ожидая, что `subscriptionManager.SubscribeWithRetryAsync` будет вызван. Однако в реальной реализации [`BaseWebSocketClient.ConnectAsync`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:112):
1. Проверяет `IsConnected` — если `true`, выходит досрочно
2. Вызывает `_connectionManager.ConnectAsync(GetWebSocketUri(), cancellationToken)`
3. Вызывает `OnConnected()`
4. Вызывает `StartReceiveLoopAsync(cancellationToken)`
5. Вызывает `_subscriptionManager.SubscribeWithRetryAsync(Symbol, cancellationToken)`

В тесте `_connectionManagerMock.SetupGet(cm => cm.IsConnected).Returns(false)`, но `_connectionManagerMock.ConnectAsync` не настроен. При вызове `ConnectAsync` на моке он вернёт `Task.CompletedTask` (по умолчанию для Moq методов, возвращающих Task). Это корректно.

Однако тест использует `.GetAwaiter().GetResult()` — это синхронная блокировка, которая может вызвать дедлок в среде с `SynchronizationContext`. Тест помечен как `[Fact(Timeout = 5000)]`, но дедлок может просто привести к таймауту.

**Необходимо:** Использовать `async Task` и `await` вместо `.GetAwaiter().GetResult()`.

### Проблема 2: Тест `StartAsync_StartsBackgroundRecoveryLoop` — не проверяет запуск цикла

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:373)

**Описание:** Тест проверяет только, что `task.Should().NotBeNull()` и что лог содержит сообщение о запуске. Он не проверяет, что фоновый цикл действительно запущен и работает. В реальной реализации [`StartAsync`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:148) запускает `RunBackgroundRecoveryLoopAsync`, который в цикле вызывает `ConnectAsync`. Если `IsConnected=false`, то `ConnectAsync` вызовет `_connectionManager.ConnectAsync`, который не настроен в тесте.

**Необходимо:** Настроить `_connectionManagerMock` так, чтобы `ConnectAsync` отрабатывал, и проверить, что он был вызван.

### Проблема 3: Тест `StopAsync_StopsBackgroundRecoveryLoop` — не проверяет остановку

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:404)

**Описание:** Тест запускает `StartAsync`, ждёт 100мс, затем вызывает `StopAsync`. Проверяет только лог. Не проверяет, что фоновый цикл действительно остановлен (например, что `_backgroundRecoveryTask.IsCompleted`).

**Необходимо:** Добавить проверку, что после `StopAsync` задача фонового цикла завершена.

### Проблема 4: Тест `DisconnectAsync_WhenConnected_ClosesConnection` — не проверяет `StopReceiveLoop`

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:439)

**Описание:** Тест проверяет, что `_messageReceiver.StopReceiveLoop()` вызван. Однако в реальной реализации [`DisconnectAsync`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:199) вызов `StopReceiveLoop()` находится в блоке `finally`. Тест проверяет `Times.Once`, что корректно.

**Вывод:** Тест корректен.

### Проблема 5: Тест `DisposeAsync_DisposesResources` — неверная проверка `StateChanged`

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:673)

**Описание:** Тест проверяет:
```csharp
connectionManagerMock.VerifyAdd(cm => cm.StateChanged += It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
connectionManagerMock.VerifyRemove(cm => cm.StateChanged -= It.IsAny<EventHandler<WebSocketState>>(), Times.Once);
```

`VerifyAdd` проверяет, что к событию подписались. Но подписка происходит в **конструкторе** `BaseWebSocketClient`, а не в `DisposeAsync`. Тест создаёт новый экземпляр клиента, поэтому `VerifyAdd` проходит. Однако это проверяет конструктор, а не `DisposeAsync`.

`VerifyRemove` проверяет, что отписка произошла — это корректно для `DisposeAsync`.

**Необходимо:** Разделить проверки: проверку подписки вынести в тест конструктора, а в тесте `DisposeAsync` проверять только отписку и диспоз.

### Проблема 6: Тест `Dispose_DisposesResources` — синхронный `Dispose` может вызвать дедлок

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:706)

**Описание:** Реальная реализация [`Dispose`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:398) вызывает `StopAsync(CancellationToken.None).Wait(_options.DisposeTimeout)`. Это синхронная блокировка на асинхронном методе, что может вызвать дедлок. Тест не проверяет, что `Dispose` завершается без дедлока в разумное время.

**Необходимо:** Добавить проверку, что `Dispose` завершается в течение таймаута.

### Рекомендации:

1. **Использовать `async Task`** вместо `.GetAwaiter().GetResult()` во всех тестах.
2. **Добавить тест на повторный вызов `ConnectAsync`** — проверка идемпотентности.
3. **Добавить тест на `ConnectAsync` с отменённым токеном**.
4. **Добавить тест на `DisconnectAsync` когда соединение уже разорвано** — проверка идемпотентности.
5. **Разделить тесты `DisposeAsync` и `Dispose`** — проверять разные аспекты.