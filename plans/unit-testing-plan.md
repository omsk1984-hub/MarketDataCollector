# План unit-тестирования для MarketDataCollector

## Цель
Создать комплексные unit-тесты для всех важных классов и функций проекта, используя фреймворк xUnit. Обеспечить покрытие ключевой бизнес-логики, обработки ошибок и граничных условий.

## Структура тестового проекта
Предлагается создать новый проект в папке `tests/` с именем `MarketDataCollector.Tests`:

```
tests/
├── MarketDataCollector.Tests/
│   ├── MarketDataCollector.Tests.csproj
│   ├── Core/
│   │   ├── Clients/
│   │   │   ├── BaseWebSocketClientTests.cs
│   │   │   ├── WebSocketMessageReceiverTests.cs
│   │   │   ├── ExponentialReconnectStrategyTests.cs
│   │   │   ├── WebSocketConnectionManagerTests.cs
│   │   │   └── SubscriptionManagerTests.cs
│   │   └── Configuration/
│   │       └── WebSocketClientOptionsTests.cs
│   ├── Infrastructure/
│   │   ├── Clients/
│   │   │   └── BinanceWebSocketClientTests.cs
│   │   ├── Factories/
│   │   │   └── WebSocketClientFactoryTests.cs
│   │   └── Repositories/
│   │       └── RawTickRepositoryTests.cs
│   ├── Application/
│   │   ├── Services/
│   │   │   ├── MarketDataProcessorTests.cs
│   │   │   ├── DataStorageServiceTests.cs
│   │   │   └── MonitoringServiceTests.cs
│   │   └── (другие сервисы)
│   └── Domain/
│       └── Entities/
│           └── RawTickTests.cs
└── (существующие консольные проекты)
```

## Зависимости
- xUnit (версия 2.x)
- Moq (для создания mock-объектов)
- FluentAssertions (для удобных утверждений, опционально)
- Microsoft.Extensions.Options (для тестирования конфигурации)
- Microsoft.EntityFrameworkCore.InMemory (для тестирования репозиториев)

## Тестовые сценарии по классам

### 1. BaseWebSocketClient
**Цель:** Проверить координацию компонентов, управление жизненным циклом и обработку событий.

**Сценарии:**
- `ConnectAsync_WhenAlreadyConnected_DoesNothing`
- `ConnectAsync_WhenNotConnected_CallsConnectionManagerAndStartsReceiveLoop`
- `DisconnectAsync_WhenConnected_ClosesConnection`
- `StartAsync_StartsBackgroundRecoveryLoop`
- `StopAsync_StopsBackgroundRecoveryLoop`
- `SetSubscriptionManager_SetsManagerCorrectly`
- `OnConnectionStateChanged_RaisesAppropriateEvents`
- `Dispose_DisposesResources`

**Mock-объекты:** `IWebSocketConnectionManager`, `IWebSocketMessageReceiver`, `IReconnectStrategy`, `ISubscriptionManager`, `ILogger`

### 2. WebSocketMessageReceiver
**Цель:** Проверить сбор фрагментированных сообщений, обработку ошибок и управление буфером.

**Сценарии:**
- `StartReceiveLoopAsync_ReceivesCompleteMessage_CallsProcessMessage`
- `StartReceiveLoopAsync_MessageExceedsMaxSize_SkipsMessage`
- `StartReceiveLoopAsync_ConnectionLost_BreaksLoop`
- `StartReceiveLoopAsync_ReceiveThrowsException_CallsOnError`
- `StopReceiveLoop_StopsLoop`

**Mock-объекты:** `IWebSocketConnectionManager`, `ILogger`

### 3. ExponentialReconnectStrategy
**Цель:** Проверить вычисление задержек и логику повторных попыток.

**Сценарии:**
- `GetDelay_FirstAttempt_ReturnsBaseDelay`
- `GetDelay_ThirdAttempt_ReturnsExponentialDelay`
- `GetDelay_ExceedsMaxDelay_ReturnsCappedDelay`
- `ShouldRetry_AlwaysReturnsTrue`
- `Reset_LogsDebug`

**Mock-объекты:** `ILogger`, `IOptions<WebSocketClientOptions>`

### 4. WebSocketConnectionManager
**Цель:** Проверить потокобезопасное управление WebSocket-соединением.

**Сценарии:**
- `ConnectAsync_WhenNotConnected_ConnectsSuccessfully`
- `ConnectAsync_WhenAlreadyConnected_DoesNothing`
- `DisconnectAsync_WhenOpen_ClosesGracefully`
- `SendAsync_WhenNotOpen_ThrowsInvalidOperationException`
- `ReceiveAsync_WhenNotOpen_ThrowsInvalidOperationException`
- `StateChanged_EventRaisedOnStateChange`
- `Dispose_DisposesWebSocket`

**Mock-объекты:** `ILogger` (реальный WebSocket мокается через подкласс или wrapper)

### 5. SubscriptionManager
**Цель:** Проверить политику повторных попыток подписки с использованием Polly.

**Сценарии:**
- `SubscribeWithRetryAsync_Success_NoRetries`
- `SubscribeWithRetryAsync_ThrowsException_RetriesUpToMax`
- `SubscribeWithRetryAsync_AllRetriesExhausted_Throws`
- `SubscribeWithRetryAsync_CancellationToken_CancelsOperation`

**Mock-объекты:** `IWebSocketConnectionManager`, `ILogger`, `Func<string, CancellationToken, Task>`

### 6. BinanceWebSocketClient
**Цель:** Проверить специфичную логику Binance: парсинг сообщений и формирование URI.

**Сценарии:**
- `GetWebSocketUri_ReturnsConstructorUri`
- `SubscribeToTickerAsync_SendsCorrectJsonMessage`
- `ProcessMessageAsync_ValidTradeMessage_CallsDataProcessor`
- `ProcessMessageAsync_NonTradeMessage_DoesNothing`
- `ProcessMessageAsync_InvalidJson_LogsError`

**Mock-объекты:** `IMarketDataProcessor`, `IWebSocketConnectionManager`, `IWebSocketMessageReceiver`, `IReconnectStrategy`, `ILogger`

### 7. MarketDataProcessor
**Цель:** Проверить пакетную обработку тиков, работу с Channel и обработку ошибок.

**Сценарии:**
- `ProcessTickAsync_WritesToChannel`
- `StartProcessingAsync_StartsBackgroundTask`
- `ProcessBatchesAsync_AccumulatesBatchAndSaves`
- `ProcessBatchesAsync_ChannelEmpty_Waits`
- `ProcessBatchesAsync_RepositoryThrows_InvokesOnError`
- `StopProcessingAsync_StopsTask`

**Mock-объекты:** `IRawTickRepository`, `ITimeService`, `ILogger`

### 8. DataStorageService
**Цель:** Проверить бизнес-логику работы с данными (агрегация, проверка дубликатов).

**Сценарии:**
- `StoreTickAsync_ValidTick_SavesToRepository`
- `StoreTickAsync_DuplicateTick_ReturnsFalse`
- `GetTicksByTickerAsync_ReturnsFilteredTicks`
- `GetTicksByExchangeAsync_ReturnsFilteredTicks`
- `TickExistsAsync_ChecksRepository`

**Mock-объекты:** `IRawTickRepository`, `ITimeService`, `ILogger`

### 9. RawTickRepository (интеграционные тесты)
**Цель:** Проверить взаимодействие с Entity Framework Core.

**Подход:** Использовать InMemory database для изоляции.

**Сценарии:**
- `AddAsync_SavesEntity`
- `GetByTickerAsync_ReturnsMatchingTicks`
- `ExistsAsync_ReturnsTrueForExistingTick`
- `GetByExchangeAsync_WithDateRange_FiltersCorrectly`

**Зависимости:** `MarketDataDbContext` (InMemory), `SystemTimeService`

### 10. WebSocketClientFactory
**Цель:** Проверить создание клиентов с правильными зависимостями.

**Сценарии:**
- `CreateBinanceClient_ReturnsBinanceWebSocketClient`
- `CreateAllClients_ReturnsClientsForAllConfigurations`
- `CreateBinanceClient_WithInvalidConfig_Throws`

**Mock-объекты:** `IServiceProvider`, `IOptions<ExchangeConfig>`, `IMarketDataProcessor`

### 11. MonitoringService
**Цель:** Проверить отслеживание состояния соединений и счетчиков тиков.

**Сценарии:**
- `UpdateConnectionStatus_UpdatesStatus`
- `IncrementTickCounter_IncrementsCount`
- `GetConnectionStatus_ReturnsCurrentStatus`
- `GetTickCount_ReturnsCount`
- `ResetCounters_ResetsAllCounters`

## Общие рекомендации

### Mocking
- Использовать Moq для создания mock-объектов интерфейсов.
- Для `ILogger` использовать `Mock<ILogger<T>>` с проверкой вызовов `Log`.
- Для `IOptions<T>` использовать `Options.Create(new T())`.

### Тестовые данные
- Создать фабрики тестовых данных (TestDataFactory) для генерации сущностей `RawTick`, `ExchangeConfig` и т.д.
- Использовать Faker или ручное создание.

### Асинхронные тесты
- Все тесты должны быть асинхронными (`public async Task`).
- Использовать `await` и `Task.CompletedTask` для mock-методов.

### Проверка исключений
- Использовать `Assert.ThrowsAsync<T>` или `await Assert.ThrowsAsync<T>`.

### Конфигурация
- Тестовые настройки `WebSocketClientOptions` и `ExchangeConfig` должны быть определены в каждом тестовом классе или в общем fixture.

## Покрытие кода
Целевое покрытие: **80%** по основным ветвлениям (branch coverage). Использовать Coverlet или Fine Code Coverage для отслеживания.

## Интеграционные тесты
Для компонентов, работающих с внешними зависимостями (база данных, внешние API), предусмотреть отдельный проект интеграционных тестов `MarketDataCollector.IntegrationTests`. В данном плане фокус на unit-тестах.

## Шаги реализации
1. Создать тестовый проект `MarketDataCollector.Tests`.
2. Добавить зависимости (xUnit, Moq, FluentAssertions, Microsoft.EntityFrameworkCore.InMemory).
3. Настроить структуру папок, как указано выше.
4. Реализовать тестовые классы последовательно, начиная с самых простых (ExponentialReconnectStrategy).
5. Запускать тесты после каждого класса, убеждаться в зелёном статусе.
6. Интегрировать в CI/CD (если есть).

## Ожидаемые результаты
- Повышение надёжности кода за счёт выявления скрытых дефектов.
- Упрощение рефакторинга благодаря наличию тестовой сетки.
- Документация поведения системы через тестовые сценарии.

## Примечания
- Тесты не должны зависеть от внешних сервисов (Binance WebSocket, реальная БД).
- Использовать `CancellationTokenSource` для тестирования отмены операций.
- Учитывать многопоточность в тестах для `WebSocketConnectionManager` и `MarketDataProcessor`.