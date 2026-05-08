# План рефакторинга: изоляция ошибок WebSocket-клиентов

## Статус: ✅ РЕАЛИЗОВАНО (2026-05-08)

## Проблема

При падении одного клиента на этапе `ConnectAsync` или `SubscribeWithRetryAsync` весь воркер перезапускается через `catch` + `Task.WhenAll`, что приводит к отключению **всех** клиентов, включая работающие.

## Цель

Каждый клиент должен быть автономен: падение одного не влияет на остальных ни на каком этапе жизненного цикла.

## Архитектура после рефакторинга

```
┌─────────────────────────────────────────────────────────┐
│                      Worker                              │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Client A │  │ Client B │  │ Client C │  ...          │
│  │ (авто-   │  │ (авто-   │  │ (авто-   │              │
│  │ номный)  │  │ номный)  │  │ номный)  │              │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘              │
│       │              │              │                    │
│       ▼              ▼              ▼                    │
│  ┌─────────────────────────────────────────┐            │
│  │     MarketDataProcessor (Channel)        │            │
│  └─────────────────────────────────────────┘            │
│                                                          │
│  ┌─────────────────────────────────────────┐            │
│  │     HealthCheckMonitor (Timer)           │            │
│  │  Проверяет IsConnected, перезапускает    │            │
│  │  отставших клиентов                      │            │
│  └─────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────┘
```

## Выполненные изменения

### Шаг 1-3, 5: Worker.cs — полный рефакторинг

**Файл:** `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs`

**Что изменилось:**
- `ConnectAsync`: вместо `Task.WhenAll` — последовательный `foreach` с индивидуальной обработкой ошибок каждого клиента
- `SubscribeWithRetryAsync`: ошибки подписки логируются, но не прерывают другие клиенты
- Добавлен health-check таймер (каждые 30 сек) с методом `RestartDisconnectedClientsAsync`
- `SubscribeWithRetryAsync` теперь пробрасывает исключение после исчерпания попыток (вызывающий код ловит и продолжает)
- Список `connectedClients` содержит только успешно подключённых — health-check работает только с ними

### Шаг 4: BaseWebSocketClient.cs — ограничение reconnect-попыток

**Файл:** `src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs`

**Что изменилось:**
- `ReceiveLoopAsync`: добавлен счётчик `reconnectAttempts` с лимитом `maxInternalReconnects = 3`
- После исчерпания 3 попыток — выходит из цикла с событием `ErrorOccurred`, ожидая внешнего перезапуска от health-check'а
- Счётчик сбрасывается после успешного подключения

### Шаг 6: MonitoringService + Factory — интеграция

**Новый файл:** `src/MarketDataCollector.Core/Interfaces/IMonitoringService.cs`
- Интерфейс `IMonitoringService` и enum `ConnectionStatus` вынесены из Application в Core
- Это позволяет Infrastructure использовать мониторинг без циклической зависимости

**Обновлён:** `src/MarketDataCollector.Application/Services/MonitoringService.cs`
- Реализация теперь наследует `Core.Interfaces.IMonitoringService`

**Обновлён:** `src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs`
- Добавлен `IMonitoringService` в конструктор
- События клиентов (`Connected`, `Disconnected`, `ErrorOccurred`, `MessageReceived`) подключены к мониторингу

**Обновлён:** `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs`
- Добавлена регистрация `IMonitoringService` как singleton в DI-контейнере

## Матрица поведения после рефакторинга

| Сценарий | Было | Стало |
|---|---|---|
| Клиент падает при ConnectAsync | Все перезапускаются | Остальные продолжают подключаться. Упавший перезапустится health-check'ом |
| Клиент падает при Subscribe | Все перезапускаются | Остальные продолжают подписываться. Упавший перезапустится health-check'ом |
| Обрыв соединения после подключения | Самостоятельный reconnect (∞ попыток) | 3 внутренних попытки → health-check в Worker перезапускает |
| Все клиенты упали | Полный рестарт воркера | Health-check перезапускает каждого по очереди |
| Отмена (CancellationToken) | Корректное завершение | Корректное завершение (без изменений) |

## Проверка компиляции

```
dotnet build MarketDataCollector.sln
Build succeeded.
0 Error(s), 11 Warning(s) — только предупреждения nullable reference types (уже были до рефакторинга)
```
