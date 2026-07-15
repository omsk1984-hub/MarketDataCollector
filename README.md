# Market Data Collector System

Система сбора, обработки и хранения ценовых данных с криптобирж в реальном времени.

> **Результаты нагрузочного тестирования:** 3 WebSocket-клиента (BTCUSDT, ETHUSDT, SOLUSDT) + 3 параллельных consumer'а + Binary COPY protocol. Фактическая производительность записи в PostgreSQL: **~35 000 RPS** (processed ticks/sec) при входящем потоке ~50 000 msg/s. Канал (ChannelCapacity=150000) утилизирует backlog при простое генератора. Потери уникальных данных при graceful shutdown — **0** (благодаря `_internalCts`).

## Описание

Система предназначена для непрерывного сбора тиковых данных (сделок) с криптобирж через WebSocket соединения, нормализации данных, удаления дубликатов и сохранения в базу данных PostgreSQL. Поддерживает параллельную работу с несколькими источниками данных и символами. Архитектура построена на принципах SOLID с чистыми зависимостями и делегированием ответственности специализированным компонентам.

## Функционал

### 1. Сбор данных
- WebSocket клиенты для подключения к биржам (Binance — реализована, остальные — через расширение)
- Поддержка нескольких одновременных подключений (разные символы)
- Автоматическое переподключение при обрывах соединения (экспоненциальный backoff через `IReconnectStrategy`)
- Фоновый health-check каждые 10 секунд с перезапуском отключённых клиентов
- Делегированная архитектура: `IWebSocketConnectionManager` (управление соединением), `IWebSocketMessageReceiver` (цикл приёма сообщений), `ISubscriptionManager` (подписка), `IReconnectStrategy` (переподключение)

### 2. Обработка потока данных
- Нормализация к единому формату (тикер, цена, объём, timestamp, биржа)
- **Трёхуровневая дедупликация**: в памяти (batch, `GroupBy`) + `ON CONFLICT DO NOTHING` в PostgreSQL
- Асинхронная обработка через **N независимых `Channel<T>`** — по одному на consumer (per-ticker routing через hash ticker'а)
- **Bulk insert** через Binary COPY protocol (Npgsql) — в 10-100x быстрее `AddRangeAsync`
- **Multiple Consumers Mode** (по умолчанию): каждый consumer получает disjoint набор тикеров → B-tree страницы unique-индекса не пересекаются → deadlock'и невозможны
- **Single Consumer Mode**: ровно 1 consumer, Channel с `SingleReader=true` (~62k ticks/sec)
- Обработка критических ошибок с остановкой Worker для внешнего перезапуска (Docker/K8s)

### 3. Хранение в БД
- Сохранение сырых тиков в PostgreSQL через **Binary COPY protocol** (Npgsql) + temp table + `INSERT ON CONFLICT DO NOTHING`
- Уникальный индекс `(Ticker, Exchange, Timestamp)` — финальная защита от дубликатов на уровне БД
- **Deadlock-free** параллельная запись: per-ticker routing гарантирует непересекающиеся B-tree страницы
- Retry-логика (5 попыток, exponential backoff + jitter) как safety net
- Поддержка сущности агрегированных данных (модель и таблица определены, запись — через расширение)
- Логирование подключений (сущность `ConnectionLog`) — fire-and-forget запись через `MonitoringService`

### 4. Мониторинг
- Логирование основных событий (подключение/отключение источника, ошибки)
- Счётчик обработанных тиков по каждой бирже (через `MonitoringService`)
- Периодический health-check статуса в лог (каждые 10 секунд) с метриками:
  - RPS: входящие/обработанные (через `SlidingWindowCounter`)
  - Channel fill % — заполненность канала (мониторинг перегрузки)
  - Backlog backlog (incoming - received) — тики в очереди, не дропнуты
  - Реальные дропы через DropOldest
- События `OnError` для критических ошибок процессора
- **Graceful shutdown** — consumer'ы дочитывают backlog перед остановкой через `_internalCts`

## Технологический стек

- **.NET 8** — основная платформа
- **Entity Framework Core 8** — ORM для работы с БД
- **PostgreSQL 16** — база данных
- **Apache Kafka (KRaft mode)** — брокер сообщений для развязки компонентов
- **Docker / Docker Compose** — контейнеризация (PostgreSQL, Kafka, Kafdrop)
- **Polly 8** — политики повторных попыток (референс в проекте; фактическая стратегия — собственная реализация `ExponentialReconnectStrategy`)
- **Newtonsoft.Json** — парсинг JSON сообщений бирж
- **Npgsql** — драйвер PostgreSQL для .NET
- **WebSocket (`System.Net.WebSockets`)** — протокол для реального времени
- **xUnit + Moq + FluentAssertions** — модульное тестирование

## Структура проекта

```
MarketDataCollector/
├── src/
│   ├── MarketDataCollector.Core/              # Интерфейсы, базовые классы, конфигурация
│   │   ├── Clients/
│   │   │   ├── BaseWebSocketClient.cs         # Базовый координатор WebSocket-клиента
│   │   │   ├── WebSocketConnectionManager.cs  # Управление соединением
│   │   │   ├── WebSocketMessageReceiver.cs    # Цикл приёма сообщений
│   │   │   ├── ExponentialReconnectStrategy.cs# Стратегия экспоненциального переподключения
│   │   │   ├── SubscriptionManager.cs         # Менеджер подписки с retry
│   │   │   └── ClientWebSocketWrapper.cs      # Обёртка над ClientWebSocket
│   │   ├── Configuration/
│   │   │   ├── ExchangeConfig.cs              # Модели конфигурации бирж (ExchangeOptions)
│   │   │   ├── WebSocketClientOptions.cs      # Параметры WebSocket-клиента
│   │   │   └── MarketDataProcessorOptions.cs  # Параметры процессора
│   │   └── Interfaces/                        # Все интерфейсы системы
│   │       ├── IExchangeWebSocketClient.cs    # WebSocket клиент биржи
│   │       ├── IWebSocketClient.cs            # Базовый WebSocket клиент
│   │       ├── IClientWebSocket.cs            # Абстракция ClientWebSocket
│   │       ├── IWebSocketConnectionManager.cs # Менеджер соединения
│   │       ├── IWebSocketMessageReceiver.cs   # Приёмник сообщений
│   │       ├── IReconnectStrategy.cs          # Стратегия переподключения
│   │       ├── ISubscriptionManager.cs        # Менеджер подписки
│   │       ├── IWebSocketClientFactory.cs     # Фабрика клиентов
│   │       ├── IMarketDataProcessor.cs        # Процессор рыночных данных
│   │       ├── IMonitoringService.cs          # Сервис мониторинга
│   │       ├── IRawTickRepository.cs          # Репозиторий тиков
│   │       ├── IConnectionLogRepository.cs    # Репозиторий логов подключений
│   │       └── IRepository.cs                 # Базовый репозиторий
│   ├── MarketDataCollector.Domain/            # Сущности домена, доменные интерфейсы
│   │   ├── Entities/
│   │   │   ├── RawTick.cs                     # Сырой тик
│   │   │   ├── AggregatedData.cs              # Агрегированные данные (свечи)
│   │   │   └── ConnectionLog.cs               # Лог подключений
│   │   └── Interfaces/
│   │       └── ITimeService.cs                # Абстракция времени
│   ├── MarketDataCollector.Infrastructure/    # Реализации (репозитории, клиенты, фабрики)
│   │   ├── Clients/BinanceWebSocketClient.cs  # Клиент Binance
│   │   ├── Data/MarketDataDbContext.cs        # EF Core DbContext
│   │   ├── Factories/WebSocketClientFactory.cs# Фабрика WebSocket клиентов (двухфазная инициализация)
│   │   ├── Repositories/
│   │   │   ├── RawTickRepository.cs           # Репозиторий тиков
│   │   │   └── ConnectionLogRepository.cs     # Репозиторий логов подключений
│   │   └── Services/SystemTimeService.cs      # Реализация ITimeService
│   ├── MarketDataCollector.Application/       # Бизнес-логика, сервисы
│   │   └── Services/
│   │       ├── MarketDataProcessor.cs         # Процессор тиков (Channel + batch + дедупликация)
│   │       ├── DataStorageService.cs          # Сервис хранения данных (обёртка над репозиторием)
│   │       └── MonitoringService.cs           # Сервис мониторинга (счётчики + ConnectionLog)
│   └── MarketDataCollector.Workers/           # Фоновый сервис сбора данных
│       └── MarketDataCollector.Worker/
│           ├── Program.cs                     # Точка входа, регистрация DI
│           ├── Worker.cs                      # BackgroundService с health-check
│           ├── appsettings.json               # Конфигурация
│           ├── appsettings.Development.json   # Конфигурация для разработки
│           └── Properties/launchSettings.json
├── tests/                                     # Тестовые проекты
│   ├── MarketDataCollector.Tests/             # xUnit модульные тесты (12 файлов)
│   │   ├── Application/Services/
│   │   │   ├── MarketDataProcessorTests.cs
│   │   │   ├── DataStorageServiceTests.cs
│   │   │   └── MonitoringServiceTests.cs
│   │   ├── Core/Clients/
│   │   │   ├── BaseWebSocketClientTests.cs
│   │   │   ├── ExponentialReconnectStrategyTests.cs
│   │   │   ├── SubscriptionManagerTests.cs
│   │   │   ├── WebSocketConnectionManagerTests.cs
│   │   │   └── WebSocketMessageReceiverTests.cs
│   │   └── Infrastructure/
│   │       ├── Clients/BinanceWebSocketClientTests.cs
│   │       ├── Factories/WebSocketClientFactoryTests.cs
│   │       └── Repositories/RawTickRepositoryTests.cs
│   ├── BinanceTick/                           # Консольный монитор Binance
│   └── KrakenTick/                            # Консольный монитор Kraken
├── docker/                                    # Docker конфигурации
│   ├── docker-compose.yml                     # PostgreSQL 16 + Kafka KRaft + Kafdrop
│   ├── init.sql                               # Инициализация схемы БД
│   └── kafka/
│       └── init-topics.sh                     # Скрипт создания топиков Kafka
├── scripts/                                   # Скрипты
├── config/                                    # Дополнительные конфигурации
├── plans/                                     # Планы рефакторинга
├── docs/                                      # Документация
└── tasks/                                     # Описания задач
```

## Архитектура

### Слоистая архитектура с делегированными компонентами

```
┌──────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Worker (BackgroundService)                  │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────┐            │
│  │  Worker     │  │  Health-Check    │  │  Cleanup  │            │
│  │             │  │  (10s interval)  │  │           │            │
│  └──────┬──────┘  └──────────────────┘  └───────────┘            │
└─────────┼────────────────────────────────────────────────────────┘
          │ использует
┌─────────▼────────────────────────────────────────────────────────┐
│  MarketDataCollector.Infrastructure                              │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  WebSocketClientFactory (двухфазная инициализация)        │    │
│  │  ┌────────────────────────────────────────────────────┐   │    │
│  │  │  BinanceWebSocketClient (IExchangeWebSocketClient)  │   │    │
│  │  │  ┌────────────────┐ ┌───────────────┐              │   │    │
│  │  │  │ ConnectionMgr  │ │ MessageRcvr   │              │   │    │
│  │  │  ├────────────────┤ ├───────────────┤              │   │    │
│  │  │  │ ReconnectStrat │ │ SubscriptionMgr│              │   │    │
│  │  │  └────────────────┘ └───────────────┘              │   │    │
│  │  └────────────────────────────────────────────────────┘   │    │
│  └──────────────────────────────────────────────────────────┘    │
│  ┌────────────────────┐  ┌───────────────────────────────────┐   │
│  │ RawTickRepository  │  │ MarketDataDbContext               │   │
│  │ ConnectionLogRepo  │  │ (EF Core: RawTicks, ConnectionLogs,│   │
│  └────────────────────┘  │  AggregatedData)                  │   │
│                          └───────────────────────────────────┘   │
│  ┌────────────────────┐                                          │
│  │ SystemTimeService  │                                          │
│  └────────────────────┘                                          │
└──────────────────────────────────────────────────────────────────┘
          ▲ реализует интерфейсы
┌─────────┴────────────────────────────────────────────────────────┐
│  MarketDataCollector.Core (Interfaces + BaseWebSocketClient)     │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │  IExchangeWebSocketClient ← BaseWebSocketClient          │    │
│  │  IWebSocketConnectionManager ← WebSocketConnectionManager│    │
│  │  IWebSocketMessageReceiver  ← WebSocketMessageReceiver   │    │
│  │  IReconnectStrategy         ← ExponentialReconnectStrat  │    │
│  │  ISubscriptionManager       ← SubscriptionManager        │    │
│  │  IWebSocketClientFactory    ← (интерфейс фабрики)         │    │
│  │  IMarketDataProcessor       ← (интерфейс процессора)      │    │
│  │  IMonitoringService         ← (интерфейс мониторинга)     │    │
│  └──────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
          │ использует
┌─────────▼────────────────────────────────────────────────────────┐
│  MarketDataCollector.Application                                 │
│  ┌───────────────────┐  ┌────────────────────────────┐           │
│  │MarketDataProcessor│  │ MonitoringService          │           │
│  │(Channel + Batch)  │  │ (Counters + Status +       │           │
│  │двухуровневая      │  │  ConnectionLog fire&forget)│           │
│  │дедупликация)      │  └────────────────────────────┘           │
│  └───────────────────┘                                          │
│  ┌───────────────────┐                                          │
│  │DataStorageService │  (обёртка над IRawTickRepository)         │
│  └───────────────────┘                                          │
└──────────────────────────────────────────────────────────────────┘
          ▲ реализует интерфейсы Domain
┌─────────┴────────────────────────────────────────────────────────┐
│  MarketDataCollector.Domain                                      │
│  ┌──────────┐  ┌───────────────┐  ┌──────────────────┐           │
│  │ RawTick  │  │ AggregatedData│  │ ConnectionLog    │           │
│  └──────────┘  └───────────────┘  └──────────────────┘           │
│  ┌────────────────────┐                                         │
│  │ ITimeService       │                                         │
│  └────────────────────┘                                         │
└──────────────────────────────────────────────────────────────────┘
```

### Поток данных

```
Binance WebSocket
    ↓ (сырые JSON сообщения)
BinanceWebSocketClient.ProcessMessageAsync()
    ↓ (парсинг в TickData)
IWebSocketMessageReceiver (цикл приёма)
    ↓ (вызов ProcessMessageAsync)
IMarketDataProcessor.ProcessTickAsync()
    ↓ (per-ticker routing: hash(ticker) % consumerCount)
N независимых Channel<TickData> (SingleReader=true)
    ↓
N Consumer'ов (каждый читает свой канал)
    ↓ (накопление батча, _batchSize = 1000)
Дедупликация:
    1. GroupBy в памяти (Ticker, Exchange, Timestamp) — внутри батча
    2. ON CONFLICT DO NOTHING — глобально, через unique-индекс БД
    ↓
RawTickRepository.BulkCopyAsync()
    ↓ (Binary COPY → temp table → INSERT ON CONFLICT)
PostgreSQL (RawTicks)
```

### Принципы SOLID
1. **Single Responsibility** — каждый класс имеет одну ответственность: `WebSocketConnectionManager` — соединение, `WebSocketMessageReceiver` — приём, `ExponentialReconnectStrategy` — переподключение, `SubscriptionManager` — подписка
2. **Open/Closed** — новые биржи добавляются через наследование от `BaseWebSocketClient`
3. **Liskov Substitution** — клиенты бирж взаимозаменяемы через `IExchangeWebSocketClient`
4. **Interface Segregation** — интерфейсы разделены по функциональности (12 интерфейсов вместо одного монолитного)
5. **Dependency Inversion** — все зависимости через интерфейсы и DI-контейнер

### Паттерны проектирования
- **Repository** — для доступа к данным (`IRawTickRepository`, `IConnectionLogRepository`)
- **Factory** — для создания WebSocket клиентов (`WebSocketClientFactory`) с двухфазной инициализацией
- **Observer** — события WebSocket (`MessageReceived`, `Connected`, `Disconnected`, `ErrorOccurred`)
- **Strategy** — стратегия переподключения (`IReconnectStrategy`)
- **Channel** — N независимых асинхронных очередей с backpressure (per-ticker routing через hash ticker'а)
- **Bridge** — разделение монолитного клиента на связанные, но независимые иерархии (ConnectionManager, MessageReceiver, SubscriptionManager, ReconnectStrategy)
- **Bulk Copy** — Binary COPY protocol (Npgsql) для массовой вставки (10-100x быстрее AddRangeAsync)

## Быстрый старт

### Предварительные требования

1. **.NET 8 SDK** — [скачать](https://dotnet.microsoft.com/download/dotnet/8.0)
2. **Docker Desktop** — [скачать](https://www.docker.com/products/docker-desktop)

### Шаг 1: Запуск инфраструктуры в Docker

```bash
cd docker
docker-compose up -d
```

Эта команда запускает все необходимые сервисы:

| Сервис | Назначение | Порт |
|--------|------------|------|
| **PostgreSQL 16** | База данных для хранения тиков | `localhost:5433` |
| **Kafka (KRaft)** | Брокер сообщений для развязки компонентов | `localhost:9092` (внутренний) / `localhost:9094` (внешний) |
| **Kafdrop** | Веб-интерфейс для просмотра Kafka | `http://localhost:9000` |
| **Aspire Dashboard** | OpenTelemetry Dashboard (метрики, трейсинг, логи) | OTLP gRPC: `localhost:18889`, UI: `http://localhost:19000` |

Проверьте, что все контейнеры запущены:
```bash
docker ps
```

Ожидаемый вывод:
```
CONTAINER ID   IMAGE                         PORTS                              NAMES
abc123         postgres:16-alpine            0.0.0.0:5433->5432/tcp             marketdata-postgres
def456         bitnami/kafka:latest          0.0.0.0:9092->9092/tcp,9094/tcp   marketdata-kafka
ghi789         obsidiandynamics/kafdrop:latest 0.0.0.0:9000->9000/tcp          marketdata-kafdrop
```

**Доступ к PostgreSQL:**
- **Хост**: `localhost:5433`
- **База**: `MarketDataDb`
- **Пользователь**: `marketdata_user`
- **Пароль**: `StrongPassword123!`

**Доступ к Kafka:**
- **Внутренний** (из Docker-сети): `kafka:9092`
- **Внешний** (с хост-машины): `localhost:9094`

**Доступ к Kafdrop (веб-интерфейс Kafka):**
- Откройте в браузере: http://localhost:9000
- Просматривайте топики, сообщения и consumer groups

**Доступ к Aspire Dashboard (OpenTelemetry):**
- Откройте в браузере: http://localhost:19000
- Просматривайте **Traces** (трейсинг EF Core), **Metrics** (метрики .NET Runtime, RPS) и **Logs** (структурированные логи приложения)
- Данные поступают через OTLP gRPC на порт `localhost:18889`

### Топики Kafka

При старте автоматически создаются следующие топики:

| Топик | Партиции | Назначение |
|-------|----------|------------|
| `raw-ticks` | 3 | Сырые тиковые данные с бирж |
| `aggregated-data` | 3 | OHLCV-свечи после агрегации |
| `connection-events` | 1 | События подключений/отключений |

Проверить топики можно через Kafdrop или CLI:

```bash
docker exec marketdata-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Шаг 2: Сборка решения

```bash
dotnet restore
dotnet build
```

### Шаг 3: Настройка конфигурации

Файл [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json):

```json
{
  "ConnectionStrings": {
    "MarketDataDb": "Host=localhost;Port=5433;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!"
  },
  "MarketDataProcessor": {
"BatchSize": 1000,
"ChannelCapacity": 150000,
"UseSingleConsumer": false,
"ConsumerCount": 3,
"FlushIntervalSeconds": 3
},
  "ExchangeOptions": {
    "Exchanges": [
      {
        "ExchangeName": "binance",
        "WebSocketUrl": "wss://stream.binance.com:9443/ws/{symbol}@trade"
      }
    ],
    "Readers": [
      { "ExchangeName": "binance", "Symbol": "btcusdt" },
      { "ExchangeName": "binance", "Symbol": "ethusdt" },
      { "ExchangeName": "binance", "Symbol": "solusdt" },
      { "ExchangeName": "binance", "Symbol": "xrpusdt" },
      { "ExchangeName": "binance", "Symbol": "adausdt" }
    ]
  },
  "WebSocketClient": {
    "ReconnectDelay": "00:00:05",
    "MaxReconnectDelay": "00:00:60",
    "MaxInternalReconnectAttempts": 3,
    "MaxSubscribeRetries": 3,
    "ReceiveBufferSize": 4096,
    "MaxMessageSize": 1048576,
    "DisposeTimeout": "00:00:05"
  }
}
```

**Параметры конфигурации MarketDataProcessor:**
- `BatchSize` — размер батча для записи в БД через Binary COPY (по умолчанию 1000)
- `ChannelCapacity` — ёмкость КАЖДОГО канала (150000). При N consumer'ов общая ёмкость = N × ChannelCapacity
- `UseSingleConsumer` — `true`: 1 consumer, `false`: несколько consumer'ов (по умолчанию)
- `ConsumerCount` — количество consumer'ов (0 = авто, Math.Clamp(CPU/2, 1, 4))
- `FlushIntervalSeconds` — сброс частичных батчей по таймеру (3с)
- `ExchangeOptions.Exchanges` — массив бирж с шаблонами WebSocket URL
- `ExchangeOptions.Readers` — массив ридеров (пара биржа + символ для подписки)
- `WebSocketClient.ReconnectDelay` — начальная задержка переподключения (5с)
- `WebSocketClient.MaxReconnectDelay` — максимальная задержка (60с, cap экспоненты)
- `WebSocketClient.MaxInternalReconnectAttempts` — попыток переподключения (3)
- `WebSocketClient.MaxSubscribeRetries` — попыток подписки (3)
- `WebSocketClient.ReceiveBufferSize` — размер буфера приёма (4096 байт)
- `WebSocketClient.MaxMessageSize` — макс. размер сообщения (1 МБ)
- `WebSocketClient.DisposeTimeout` — таймаут остановки (5с)

### Шаг 4: Запуск воркера сбора данных

```bash
cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
dotnet run
```

Также можно использовать скрипт [`run.ps1`](run.ps1):
```powershell
.\run.ps1
```

**Ожидаемый вывод при успешном запуске:**

```
info: MarketDataCollector.Worker.Worker[0]
      Worker starting...
info: MarketDataCollector.Worker.Worker[0]
      Starting 1 WebSocket clients...
info: MarketDataCollector.Worker.Worker[0]
      Health-check: 1 connected, 0 disconnected
```

Воркер работает непрерывно, автоматически переподключаясь при обрывах соединения.

## Добавление новой биржи

1. Создайте класс клиента, наследуемый от `BaseWebSocketClient`:
```csharp
public class NewExchangeWebSocketClient : BaseWebSocketClient
{
    private readonly IMarketDataProcessor _dataProcessor;

    public NewExchangeWebSocketClient(
        Uri webSocketUri, string exchangeName, string symbol,
        IMarketDataProcessor dataProcessor,
        IWebSocketConnectionManager connectionManager,
        IWebSocketMessageReceiver messageReceiver,
        IReconnectStrategy reconnectStrategy,
        IOptions<WebSocketClientOptions> options,
        ILogger<BaseWebSocketClient> logger)
        : base(webSocketUri, exchangeName, symbol,
               connectionManager, messageReceiver, reconnectStrategy, options, logger)
    {
        _dataProcessor = dataProcessor;
    }

    protected override Task ProcessMessageAsync(string message)
    {
        // Парсинг специфичного формата биржи
        // Вызов _dataProcessor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);
        return Task.CompletedTask;
    }
}
```

2. Добавьте фабричный метод в [`WebSocketClientFactory`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs)
3. Добавьте конфигурацию в `appsettings.json` (секции `Exchanges` и `Readers`)
4. Клиент автоматически получит мониторинг через подписку на события в фабрике

## База данных

### Схема

| Таблица | Описание |
|---------|----------|
| `RawTicks` | Сырые тиковые данные с бирж |
| `ConnectionLogs` | Логи подключений к источникам данных |
| `AggregatedData` | Агрегированные данные по интервалам (свечи) |

### Ключевые индексы
- `RawTicks`: уникальный индекс `(Ticker, Exchange, Timestamp)` — защита от дубликатов
- `RawTicks`: индексы по `Ticker`, `Timestamp`, `Exchange` — быстрый поиск
- `ConnectionLogs`: индексы по `Exchange`, `CreatedAt`
- `AggregatedData`: индексы по `(Ticker, Interval)`, `StartTime`

Схема создаётся автоматически через [`docker/init.sql`](docker/init.sql) и EF Core `OnModelCreating` в [`MarketDataDbContext`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs).

## Мониторинг и логирование

### Логи
- Уровни: Debug, Information, Warning, Error, Critical
- Выход: Console (настраивается через `appsettings.json`)
- События: подключение, отключение, ошибки, статистика, health-check

### Метрики (через [`MonitoringService`](src/MarketDataCollector.Application/Services/MonitoringService.cs))
- Количество обработанных тиков по каждой бирже
- Статус подключений (Connected / Disconnected / Error)
- Общее количество обработанных тиков
- Fire-and-forget запись событий в `ConnectionLogs`

### Health-check (в [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs))
- Периодическая проверка каждые 10 секунд
- Автоматический перезапуск отключённых клиентов (через идемпотентный `StartAsync`)
- Логирование статуса: подключено/отключено
- Остановка Worker при критической ошибке `MarketDataProcessor`

## Тестирование

### Модульные тесты (xUnit)

Проект [`tests/MarketDataCollector.Tests/`](tests/MarketDataCollector.Tests/) содержит 250+ модульных тестов:

| Категория | Файлы | Описание |
|-----------|-------|----------|
| **Application** | `MarketDataProcessorTests.cs` (34 теста), `DataStorageServiceTests.cs`, `MonitoringServiceTests.cs` | Бизнес-логика |
| **Core/Clients** | `BaseWebSocketClientTests.cs`, `ExponentialReconnectStrategyTests.cs`, `SubscriptionManagerTests.cs`, `WebSocketConnectionManagerTests.cs`, `WebSocketMessageReceiverTests.cs` | Базовые компоненты |
| **Infrastructure** | `BinanceWebSocketClientTests.cs`, `WebSocketClientFactoryTests.cs`, `RawTickRepositoryTests.cs` | Реализации |
| **Infrastructure/Kafka** | `KafkaIntegrationTests.cs`, `KafkaRealConnectionTests.cs` | Kafka + Testcontainers |

**Запуск тестов:**

```powershell
.\run_test.ps1
```

или вручную:

```bash
cd tests/MarketDataCollector.Tests
dotnet test
```

**Технологии тестирования:**
- **xUnit** — фреймворк
- **Moq** — мокирование зависимостей
- **FluentAssertions** — читаемые утверждения
- **EF Core InMemory** — тестирование репозиториев без реальной БД
- **Testcontainers** — интеграционные тесты с реальным Kafka/PostgreSQL в Docker
- Таймауты: 5000ms для WebSocket/сетевых тестов, 10000ms для Repository/DataStorage

### Тестовые мониторы

Консольные приложения для мониторинга тиков без записи в БД:

```bash
# Монитор Binance
cd tests/BinanceTick
dotnet run

# Монитор Kraken
cd tests/KrakenTick
dotnet run
```

## Разработка

### Скрипты

- [`run.ps1`](run.ps1) — сборка и запуск воркера
- [`run_test.ps1`](run_test.ps1) — запуск тестов

### Переменные окружения

Для продакшена рекомендуется использовать переменные окружения или User Secrets:

```bash
dotnet user-secrets init
dotnet user-secrets set "ConnectionStrings:MarketDataDb" "Host=...;Password=..."
```

## Устранение неполадок

### Проблема: Не подключается к бирже
**Решение:**
1. Проверьте интернет-соединение
2. Убедитесь, что URL биржи корректен
3. Проверьте наличие блокировок фаерволом
4. Проверьте логи на наличие ошибок

### Проблема: Не работает подключение к PostgreSQL
**Решение:**
1. Убедитесь, что Docker-контейнер запущен: `docker ps`
2. Проверьте порт (по умолчанию 5433)
3. Проверьте строку подключения в `appsettings.json`
4. Проверьте логи контейнера: `docker logs marketdata-postgres`

### Проблема: Дубликаты в БД
**Решение:**
1. Проверьте уникальный индекс `(Ticker, Exchange, Timestamp)` в БД
2. Убедитесь в корректности timestamp
3. Проверьте логику двухуровневой дедупликации в [`MarketDataProcessor.ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:137)

### Проблема: Воркер падает с ошибкой
**Решение:**
1. Проверьте логи на наличие `Critical` ошибок
2. Убедитесь, что процессор не завершился с ошибкой (`OnError` event)
3. Перезапустите воркер — он восстановит соединения
4. Внешний оркестратор (Docker/K8s) автоматически перезапустит Worker

### Проблема: Kafka не стартует (контейнер падает)
**Решение:**
1. Проверьте логи: `docker logs marketdata-kafka`
2. Убедитесь, что том `kafka_data` не повреждён: `docker-compose down -v && docker-compose up -d` (внимание: удалит все данные Kafka)
3. Проверьте, что в системе достаточно памяти (Kafka требует минимум 2GB RAM в Docker Desktop)
4. При первом запуске Kafka может стартовать до 30 секунд — дождитесь `healthy` статуса

### Проблема: Не создались топики Kafka
**Решение:**
1. Проверьте, что Kafka полностью запущена: `docker ps | findstr kafka`
2. Проверьте логи init-контейнера: `docker logs marketdata-kafka-init`
3. Создайте топики вручную через Kafdrop (http://localhost:9000) или CLI:
```bash
docker exec marketdata-kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic raw-ticks --partitions 3 --replication-factor 1
```
4. Перезапустите init-контейнер: `docker-compose up -d kafka-init-topics`

### Проблема: Не открывается Kafdrop (http://localhost:9000)
**Решение:**
1. Проверьте, что контейнер запущен: `docker ps | findstr kafdrop`
2. Проверьте логи: `docker logs marketdata-kafdrop`
3. Убедитесь, что порт 9000 не занят другим приложением
4. Kafdrop может стартовать с задержкой — обновите страницу через 10-15 секунд

## Лицензия

MIT License

## Контакты

Для вопросов и предложений создавайте issue в репозитории проекта.

telegram: @Omsk1984

---

*Последнее обновление: май 2026*
