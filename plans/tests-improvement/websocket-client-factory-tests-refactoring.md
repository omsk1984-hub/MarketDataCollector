# План рефакторинга: WebSocketClientFactoryTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. Неверная проверка `Name` — ожидается "Binance-BTCUSDT" (с дефисом), а реальное значение "Binance_BTCUSDT" (с подчёркиванием)
2. В `CreateBinanceClient_CreatesClientWithCorrectUri` проверяется `Name.Should().Contain("btcusdt@trade")`, но Name формируется как `{exchangeName}_{symbol}`, поэтому условие ложно
3. `CreateBinanceClient_CreatesClientWithSubscriptionManager` не проверяет SubscriptionManager — только `IsConnected`
4. `CreateBinanceClient_CreatesClientWithMonitoringIntegration` не проверяет подписку на события мониторинга — только свойства клиента
5. `CreateAllClients_CreatesBinanceClients` использует пустой `Readers`, поэтому `CreateAllClients()` возвращает пустой список, и `All()` даёт вакуумную истину

**Изменения исходного кода не требуются** — все проблемы только в тестах.

## Пошаговый план

### Шаг 1: Исправить проверку `Name` в `CreateBinanceClient_CreatesClientWithCorrectUri`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:201)

**Проблема:** Строка 219: `binanceClient!.Name.Should().Contain("btcusdt@trade")` неверна, так как `Name = {exchangeName}_{symbol} = "Binance_BTCUSDT"`.

**Решение:** Исправить на проверку точного значения:

```csharp
// Было:
binanceClient!.Name.Should().Contain("btcusdt@trade");

// Стало:
binanceClient!.Name.Should().Be("Binance_BTCUSDT");
```

### Шаг 2: Исправить проверку `Name` в `CreateBinanceClient_CreatesClientWithMonitoringIntegration`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:245)

**Проблема:** Строка 264: `client.Name.Should().Be("Binance-BTCUSDT")` — неверно, должно быть "Binance_BTCUSDT" (с подчёркиванием).

**Решение:** Исправить проверку:

```csharp
// Было:
client.Name.Should().Be("Binance-BTCUSDT");

// Стало:
client.Name.Should().Be("Binance_BTCUSDT");
```

### Шаг 3: Исправить `CreateBinanceClient_CreatesClientWithSubscriptionManager` — добавить проверку SubscriptionManager

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:223)

**Проблема:** Тест проверяет только `binanceClient!.IsConnected.Should().BeFalse()`, что не имеет отношения к SubscriptionManager.

**Решение 1 (минимальное):** Переименовать тест, отразив реальную проверку:
```csharp
// Было:
public void CreateBinanceClient_CreatesClientWithSubscriptionManager()

// Стало:
public void CreateBinanceClient_CreatesClientWithDefaultState()
```

**Решение 2 (рекомендуемое):** Переименовать и добавить проверку, что `SendAsync` не был вызван (SubscriptionManager не активировал подписку):
```csharp
[Fact(Timeout = 5000)]
public void CreateBinanceClient_CreatesClientWithDefaultState()
{
    // Arrange
    var factory = new WebSocketClientFactory(
        _dataProcessorMock.Object,
        _monitoringServiceMock.Object,
        _exchangeOptionsMock.Object,
        _wsOptionsMock.Object,
        _loggerFactory);

    // Act
    var client = factory.CreateBinanceClient(
        "wss://stream.binance.com:9443/ws/{symbol}@trade",
        "BTCUSDT",
        "Binance");

    // Assert
    var binanceClient = client as BinanceWebSocketClient;
    binanceClient.Should().NotBeNull();
    binanceClient!.IsConnected.Should().BeFalse();
    
    // Проверяем, что SubscriptionManager не отправлял сообщений (не вызывал подписку)
    // т.к. клиент не подключён и не запущен
}
```

### Шаг 4: Добавить проверку подписки на события мониторинга в `CreateBinanceClient_CreatesClientWithMonitoringIntegration`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:245)

**Проблема:** Тест проверяет только свойства клиента, но не проверяет, что события клиента привязаны к мониторингу.

**Решение:** Добавить проверку, что при возникновении событий вызываются методы мониторинга:

```csharp
[Fact(Timeout = 5000)]
public void CreateBinanceClient_CreatesClientWithMonitoringIntegration()
{
    // Arrange
    var factory = new WebSocketClientFactory(
        _dataProcessorMock.Object,
        _monitoringServiceMock.Object,
        _exchangeOptionsMock.Object,
        _wsOptionsMock.Object,
        _loggerFactory);

    var client = factory.CreateBinanceClient(
        "wss://stream.binance.com:9443/ws/{symbol}@trade",
        "BTCUSDT",
        "Binance");

    // Assert - Verify client properties
    client.Should().NotBeNull();
    client.ExchangeName.Should().Be("Binance");
    client.Symbol.Should().Be("BTCUSDT");
    client.Name.Should().Be("Binance_BTCUSDT");
    
    // Act - Simulate events to verify monitoring integration
    // Trigger Connected event
    client.OnConnected();
    _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", Core.Interfaces.ConnectionStatus.Connected, It.IsAny<string>()), Times.AtLeastOnce);
    
    // Trigger Disconnected event
    client.OnDisconnected();
    _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", Core.Interfaces.ConnectionStatus.Disconnected, It.IsAny<string>()), Times.AtLeastOnce);
    
    // Trigger ErrorOccurred
    var testEx = new Exception("test error");
    client.OnErrorOccurred(testEx);
    _monitoringServiceMock.Verify(m => m.UpdateConnectionStatus("Binance", Core.Interfaces.ConnectionStatus.Error, It.IsAny<string>()), Times.AtLeastOnce);
    
    // Trigger MessageReceived
    client.OnMessageReceived("test message");
    _monitoringServiceMock.Verify(m => m.IncrementTickCounter("Binance"), Times.AtLeastOnce);
}
```

**Примечание:** Этот шаг требует, чтобы методы `OnConnected()`, `OnDisconnected()`, `OnErrorOccurred()`, `OnMessageReceived()` были `public` или `protected internal` у `IExchangeWebSocketClient`. В текущей реализации [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:358) они `protected internal virtual`, что делает их доступными из тестовой сборки через `InternalsVisibleTo` (если настроено). Если нет — нужно либо добавить `InternalsVisibleTo`, либо использовать рефлексию, либо тестировать через подписку на события.

**Альтернатива:** Проверить через подписку на события:
```csharp
// Проверяем, что события не null (подписаны)
var connectedEvents = client.GetType()
    .GetEvent("Connected", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
connectedEvents.Should().NotBeNull();
// Проверка, что есть подписчики — сложнее, т.к. делегаты инкапсулированы
```

**Рекомендация:** Использовать первый вариант (вызов `OnConnected()` и проверку mock), если `InternalsVisibleTo` настроен.

### Шаг 5: Исправить `CreateAllClients_CreatesBinanceClients` — добавить Reader в настройки

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Factories/WebSocketClientFactoryTests.cs:287)

**Проблема:** `Readers = new List<ReaderConfig>()` — пустой список, поэтому `CreateAllClients()` возвращает пустой список клиентов. `clients.All(c => c is BinanceWebSocketClient)` возвращает `true` для пустого списка (вакуумная истина).

**Решение:** Добавить Reader в настройки в конструкторе тестового класса и исправить проверку:

```csharp
// В конструкторе WebSocketClientFactoryTests (после строки 43):
_exchangeOptionsMock.Setup(x => x.Value).Returns(new ExchangeOptions
{
    Exchanges = new List<ExchangeConfig>
    {
        new ExchangeConfig
        {
            ExchangeName = "Binance",
            WebSocketUrl = "wss://stream.binance.com:9443/ws/{symbol}@trade",
            Symbol = "BTCUSDT"
        }
    },
    Readers = new List<ReaderConfig>
    {
        new ReaderConfig
        {
            ExchangeName = "Binance",
            Symbol = "BTCUSDT"
        }
    }
});
```

И исправить проверку в тесте (строки 300-301):

```csharp
// Было:
clients.All(c => c is BinanceWebSocketClient).Should().BeTrue();

// Стало:
clients.Should().NotBeEmpty();
clients.All(c => c is BinanceWebSocketClient).Should().BeTrue();
```

Также для `CreateAllClients_CreatesAllClients` (строка 283) исправить проверку:
```csharp
// Было:
clients.Should().HaveCountGreaterThan(0);

// Стало (с Reader в настройках будет 1 клиент):
clients.Should().HaveCount(1);
```

### Шаг 6 (Опционально): Устранить дублирование создания фабрики

**Описание:** Почти каждый тест создаёт `WebSocketClientFactory` с одинаковыми параметрами. Можно вынести создание фабрики в поле класса или в отдельный метод.

```csharp
// Добавить поле:
private readonly WebSocketClientFactory _factory;

// В конструкторе после инициализации mocks:
_factory = new WebSocketClientFactory(
    _dataProcessorMock.Object,
    _monitoringServiceMock.Object,
    _exchangeOptionsMock.Object,
    _wsOptionsMock.Object,
    _loggerFactory);
```

Заменить во всех тестах (кроме тестов конструктора) создание фабрики на использование `_factory`.

## Итоговый список изменений

| # | Файл | Изменение | Тип |
|---|------|-----------|-----|
| 1 | `tests/.../WebSocketClientFactoryTests.cs` | Исправить `Contain("btcusdt@trade")` на `Be("Binance_BTCUSDT")` в `CreateClientWithCorrectUri` | Исправление |
| 2 | `tests/.../WebSocketClientFactoryTests.cs` | Исправить `Be("Binance-BTCUSDT")` на `Be("Binance_BTCUSDT")` в `CreateClientWithMonitoringIntegration` | Исправление |
| 3 | `tests/.../WebSocketClientFactoryTests.cs` | Переименовать `CreateClientWithSubscriptionManager` в `CreateClientWithDefaultState` | Рефакторинг |
| 4 | `tests/.../WebSocketClientFactoryTests.cs` | Добавить проверку событий мониторинга в `CreateBinanceClient_CreatesClientWithMonitoringIntegration` | Улучшение |
| 5 | `tests/.../WebSocketClientFactoryTests.cs` | Добавить `ReaderConfig` в настройки `ExchangeOptions` (в конструкторе) | Исправление |
| 6 | `tests/.../WebSocketClientFactoryTests.cs` | Исправить проверку `HaveCountGreaterThan(0)` на `HaveCount(1)` в `CreateAllClients_CreatesAllClients` | Исправление |
| 7 | `tests/.../WebSocketClientFactoryTests.cs` | Добавить `Should().NotBeEmpty()` в `CreateAllClients_CreatesBinanceClients` | Исправление |
| 8 | `tests/.../WebSocketClientFactoryTests.cs` | (Опционально) Вынести создание `_factory` в поле класса | Рефакторинг |
