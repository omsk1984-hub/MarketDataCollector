# План улучшения: WebSocketConnectionManagerTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`

## Результат проверки: ❌ Серьёзные проблемы

### Проблема 1: Тест `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects` — тестирует не тот код

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs:62)

**Описание:** Тест создаёт `TestableWebSocketConnectionManager`, который **переопределяет** `ConnectAsync` и вызывает `_socketFactory().ConnectAsync(uri, cancellationToken)`. Это полностью обходит реальную логику `WebSocketConnectionManager.ConnectAsync`, которая:
1. Захватывает `_connectLock`
2. Проверяет `IsConnected`
3. Создаёт новый `ClientWebSocketWrapper`
4. Вызывает `ConnectAsync` на нём
5. Атомарно заменяет старый сокет через `Interlocked.Exchange`
6. Диспозит старый сокет

Тест проверяет только, что `newWebSocketMock.ConnectAsync` был вызван, но это происходит в тестовом наследнике, а не в реальном коде.

**Необходимо:** Переписать тест так, чтобы он тестировал реальный `WebSocketConnectionManager.ConnectAsync`. Для этого нужно замокать `IClientWebSocket` так, чтобы при вызове `ConnectAsync` на нём менялось состояние на `Open`.

### Проблема 2: Тест `DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne` — неверная проверка `IsConnected`

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs:264)

**Описание:** После вызова `DisposeCurrentSocket()` тест проверяет `manager.IsConnected.Should().BeFalse()`. Однако `DisposeCurrentSocket` заменяет сокет на новый `ClientWebSocketWrapper()` через `Interlocked.Exchange`. Новый `ClientWebSocketWrapper` имеет состояние `None` (по умолчанию), а не `Closed`. `IsConnected` возвращает `true` только при `WebSocketState.Open`. Состояние `None` ≠ `Open`, поэтому `IsConnected` будет `false`. **Тест проходит, но по случайности**, а не потому что проверяет правильное поведение.

**Необходимо:** Уточнить проверку — проверить, что старый сокет был диспознут, а новый сокет создан.

### Проблема 3: Тест `StateChanged_Event_CanSubscribe` — не проверяет, что событие вызывается

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs:279)

**Описание:** Тест только подписывается на событие и проверяет, что `eventFired` остаётся `false`. Он не проверяет, что событие **действительно вызывается** при изменении состояния. Это тест "на подписку", а не на функциональность.

**Необходимо:** Добавить тест, который проверяет, что `StateChanged` вызывается после `ConnectAsync` или `DisconnectAsync`.

### Проблема 4: Тест `ConnectAsync_WhenAlreadyConnected_DoesNothing` — не проверяет блокировку

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketConnectionManagerTests.cs:88)

**Описание:** Тест проверяет, что `ConnectAsync` не вызывается на сокете, если `IsConnected=true`. Но не проверяет, что `_connectLock` освобождается корректно. В реальном коде есть `SemaphoreSlim`, и если он не освободится, будут дедлоки.

**Необходимо:** Добавить проверку, что после `ConnectAsync` можно вызвать другой метод (например, `DisconnectAsync`) без дедлока.

### Рекомендации:

1. **Переписать `ConnectAsync_WhenNotConnected_CreatesNewWebSocketAndConnects`** — убрать `TestableWebSocketConnectionManager`, тестировать реальный `WebSocketConnectionManager` с правильно настроенным mock-сокетом.
2. **Исправить `DisposeCurrentSocket_DisposesOldSocketAndCreatesNewOne`** — проверять, что старый сокет диспознут, а не полагаться на побочный эффект.
3. **Добавить тест на вызов `StateChanged`** после `ConnectAsync`.
4. **Добавить тест на потокобезопасность** — параллельные вызовы `ConnectAsync`.
5. **Удалить `TestableWebSocketConnectionManager`** после переписывания тестов.