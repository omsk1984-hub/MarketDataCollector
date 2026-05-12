# План улучшения: WebSocketClientFactoryTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `CreateBinanceClient_CreatesClientWithCorrectUri` — неверная проверка

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:201)

**Описание:** Тест проверяет:
```csharp
binanceClient!.Name.Should().Contain("btcusdt@trade");
```

В реальной реализации [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:94):
```csharp
Name = $"{exchangeName}_{symbol}";
```

Для `exchangeName="Binance"` и `symbol="BTCUSDT"` Name будет `"Binance_BTCUSDT"`. Он НЕ содержит `"btcusdt@trade"`. **Тест неверен и будет провален.**

**Необходимо:** Исправить проверку на `binanceClient!.Name.Should().Be("Binance_BTCUSDT")`.

### Проблема 2: Тест `CreateBinanceClient_CreatesClientWithMonitoringIntegration` — неверная проверка Name

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:245)

**Описание:** Тест проверяет:
```csharp
client.Name.Should().Be("Binance-BTCUSDT");
```

В реальной реализации Name = `"Binance_BTCUSDT"` (с подчёркиванием, а не дефисом). **Тест неверен и будет провален.**

**Необходимо:** Исправить на `client.Name.Should().Be("Binance_BTCUSDT")`.

### Проблема 3: Тест `CreateBinanceClient_CreatesClientWithSubscriptionManager` — не проверяет SubscriptionManager

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:223)

**Описание:** Тест проверяет только `binanceClient!.IsConnected.Should().BeFalse()`. Это не имеет отношения к SubscriptionManager. Тест должен проверять, что SubscriptionManager был установлен (например, через вызов `SubscribeWithRetryAsync` после `ConnectAsync`).

**Необходимо:** Переименовать тест или изменить проверку на более релевантную.

### Проблема 4: Тест `CreateBinanceClient_CreatesClientWithMonitoringIntegration` — не проверяет интеграцию с мониторингом

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:245)

**Описание:** Тест проверяет только свойства клиента (ExchangeName, Symbol, Name), но не проверяет, что события клиента подписаны на методы мониторинга. В реальной реализации [`CreateBinanceClient`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:79) подписывает `Connected`, `Disconnected`, `ErrorOccurred` и `MessageReceived` на методы `_monitoringService`.

**Необходимо:** Добавить проверку, что при возникновении событий клиента вызываются соответствующие методы `_monitoringService`.

### Проблема 5: Тест `CreateAllClients_CreatesBinanceClients` — может не создавать клиентов

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:287)

**Описание:** Тест проверяет `clients.All(c => c is BinanceWebSocketClient).Should().BeTrue()`. Однако в настройке `_exchangeOptionsMock` поле `Readers` пустое (`Readers = new List<ReaderConfig>()`). В реальной реализации [`CreateAllClients`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:100) итерация идёт по `_exchangeOptions.Readers`. Если Readers пуст, то `clients` будет пустым списком, и `clients.All(...)` вернёт `true` (пустое множество — вакуумная истина). **Тест проходит, но не проверяет создание клиентов.**

**Необходимо:** Добавить Reader в настройки и проверить, что клиенты действительно созданы.

### Рекомендации:

1. **Исправить проверки Name** в тестах `CreateBinanceClient_CreatesClientWithCorrectUri` и `CreateBinanceClient_CreatesClientWithMonitoringIntegration`.
2. **Добавить Reader в ExchangeOptions** для теста `CreateAllClients_CreatesBinanceClients`.
3. **Добавить проверку подписки на события мониторинга** в тест `CreateBinanceClient_CreatesClientWithMonitoringIntegration`.
4. **Добавить тест на `CreateBinanceClient` с null urlTemplate**.
5. **Добавить тест на `CreateAllClients` с несколькими биржами**.