# План улучшения: WebSocketMessageReceiverTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage` — не проверяет пропуск сообщения

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs:124)

**Описание:** Тест проверяет только, что лог содержит сообщение "Сообщение превышает максимальный размер". Он НЕ проверяет, что:
- `processMessage` НЕ был вызван для превышающего сообщения
- Цикл продолжил работу после пропуска сообщения
- Поток (`messageStream`) был очищен

В реальной реализации [`WebSocketMessageReceiver.StartReceiveLoopAsync`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:66) при превышении размера:
1. Логируется предупреждение
2. Пропускаются оставшиеся фрагменты до `EndOfMessage`
3. Поток очищается (`messageStream.SetLength(0)`)
4. Цикл продолжается (`continue`)

**Необходимо:** Добавить проверки, что `processMessage` не вызывался, и что цикл продолжился (например, следующим сообщением нормального размера).

### Проблема 2: Тест `StartReceiveLoopAsync_ConnectionLost_BreaksLoop` — не проверяет выход из цикла

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs:92)

**Описание:** Тест устанавливает `IsConnected = false` и проверяет, что лог содержит "Соединение разорвано". Однако он не проверяет, что метод действительно завершился (вышел из цикла). В реальной реализации при `IsConnected = false` выполняется `break`, и метод завершается. Тест использует `CancellationTokenSource(TimeSpan.FromSeconds(1))`, который отменяет токен через 1 секунду, что также завершает цикл. Это создаёт ложноположительный результат.

**Необходимо:** Убрать `CancellationTokenSource` с таймаутом и проверять, что метод завершился именно из-за `IsConnected = false`, а не из-за отмены токена.

### Проблема 3: Тест `StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError` — не проверяет продолжение цикла

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs:172)

**Описание:** Тест проверяет, что `onError` был вызван при ошибке `ReceiveAsync`. Однако в реальной реализации после ошибки есть пауза `Task.Delay(1000, cancellationToken)` и цикл продолжается. Тест не проверяет, что цикл продолжился после ошибки. Из-за `CancellationTokenSource(TimeSpan.FromSeconds(1))` тест может завершиться до того, как цикл сделает вторую итерацию.

**Необходимо:** Добавить проверку, что после ошибки цикл делает ещё одну попытку ReceiveAsync.

### Проблема 4: Тест `StartReceiveLoopAsync_CancellationTokenRequested_StopsLoop` — нестабильная проверка

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs:306)

**Описание:** Тест использует `cts.CancelAfter(50)` и затем `Task.WhenAny(task, Task.Delay(1000))`. Это может быть нестабильно на медленных машинах. Если задача не завершится за 50мс, тест может упасть.

**Необходимо:** Использовать предварительно отменённый токен (`new CancellationToken(true)`) для детерминированного поведения.

### Рекомендации:

1. **Добавить тест на фрагментированные сообщения** (несколько ReceiveAsync с `EndOfMessage=false`, затем `EndOfMessage=true`).
2. **Добавить тест на пустые сообщения** (Count=0).
3. **Добавить тест на бинарные сообщения** (`WebSocketMessageType.Binary`).
4. **Использовать предварительно отменённый токен** вместо `CancelAfter` для детерминизма.