# План улучшения: BinanceWebSocketClientTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `ProcessMessageAsync_InvalidJson_CallsOnErrorOccurred` — неверная проверка типа исключения

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs:339)

**Описание:** Тест проверяет:
```csharp
capturedException.Should().BeOfType<Exception>();
```

Однако в реальной реализации [`ProcessMessageAsync`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:59) при парсинге невалидного JSON `JObject.Parse(message)` выбросит `Newtonsoft.Json.JsonReaderException`, а не `Exception`. Тест проверяет `BeOfType<Exception>()`, что проходит, так как `JsonReaderException` наследуется от `Exception`. Но проверка слишком общая.

**Необходимо:** Проверять конкретный тип исключения: `capturedException.Should().BeOfType<JsonReaderException>()`.

### Проблема 2: Тест `ProcessMessageAsync_MissingFields_UsesDefaultValues` — не проверяет логирование

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs:385)

**Описание:** Тест проверяет, что при отсутствии полей `p` (price) и `q` (volume) используются значения по умолчанию `0m`. В реальной реализации:
```csharp
var price = decimal.Parse(json["p"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
var volume = decimal.Parse(json["q"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
```

При отсутствии поля `json["p"]` вернёт `null`, `?.ToString()` вернёт `null`, `?? "0"` даст "0", `decimal.Parse("0")` = 0m. **Тест корректен.**

Однако тест не проверяет, что при отсутствии полей логируется предупреждение или ошибка. В текущей реализации ошибка не логируется — просто используются нули. Это может скрывать проблемы с данными.

**Необходимо:** Добавить проверку (или изменить реализацию) на логирование предупреждения при отсутствующих полях.

### Проблема 3: Тест `SubscribeToTickerAsync_SendsCorrectJsonMessage` — создаёт лишний экземпляр клиента

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs:121)

**Описание:** Тест создаёт два экземпляра `BinanceWebSocketClient` (строки 125-134 и 142-152). Первый не используется для вызова тестируемого метода. Это избыточно и может замедлять тесты.

**Необходимо:** Убрать первый экземпляр клиента.

### Проблема 4: Тест `GetWebSocketUri_ReturnsConstructorUri` — избыточное создание клиента

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs:89)

**Описание:** Аналогично Проблеме 3 — создаётся два экземпляра клиента. Первый (строки 92-101) не используется.

**Необходимо:** Убрать первый экземпляр.

### Проблема 5: Тест `ProcessMessageAsync_WithAllFields_CallsDataProcessorWithCorrectValues` — дублирует `ProcessMessageAsync_ValidTradeMessage_CallsDataProcessor`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs:433)

**Описание:** Оба теста проверяют одно и то же — что при валидном trade-сообщении вызывается `ProcessTickAsync` с корректными значениями. Разница только в значениях полей (BTCUSDT vs ETHUSDT, 1000.50 vs 2500.75). Это дублирование.

**Необходимо:** Объединить в один параметризованный тест (`[Theory]`) или удалить дублирующийся тест.

### Рекомендации:

1. **Исправить проверку типа исключения** в `ProcessMessageAsync_InvalidJson_CallsOnErrorOccurred`.
2. **Убрать избыточные создания клиентов** во всех тестах.
3. **Объединить дублирующиеся тесты** в параметризованные.
4. **Добавить тест на `ProcessMessageAsync` с сообщением подписки** (например, `{"result":null,"id":1}`).
5. **Добавить тест на `ProcessMessageAsync` с пустым JSON** (`{}`).
6. **Добавить тест на `ProcessMessageAsync` с очень большими числами** (проверка переполнения decimal).