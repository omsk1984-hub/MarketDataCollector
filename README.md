# Market Data Collector System

Система сбора, обработки и хранения ценовых данных с криптобирж в реальном времени.

## Описание

Система предназначена для непрерывного сбора тиковых данных (сделок) с криптобирж через WebSocket соединения, нормализации данных, удаления дубликатов и сохранения в базу данных PostgreSQL. Поддерживает параллельную работу с несколькими источниками данных и символами.

## Функционал

### 1. Сбор данных
- WebSocket клиенты для подключения к биржам (Binance — реализована, остальные — через расширение)
- Поддержка нескольких одновременных подключений (разные символы)
- Автоматическое переподключение при обрывах соединения (Polly retry с экспоненциальным backoff)
- Фоновый health-check каждые 30 секунд с перезапуском отключённых клиентов

### 2. Обработка потока данных
- Нормализация к единому формату (тикер, цена, объём, timestamp, биржа)
- Удаление дубликатов: в памяти (batch) + проверка в БД по уникальному ключу (тикер + биржа + timestamp)
- Асинхронная обработка через `Channel<T>` с батчевой записью (batch size по умолчанию 100)
- Обработка критических ошибок с остановкой Worker для внешнего перезапуска (Docker/K8s)

### 3. Хранение в БД
- Сохранение сырых тиков в PostgreSQL через Entity Framework Core
- Уникальный индекс `(Ticker, Exchange, Timestamp)` для защиты от дубликатов
- Поддержка сущности агрегированных данных (модель определена, запись — через расширение)
- Логирование подключений (сущность `ConnectionLog`)

### 4. Мониторинг
- Логирование основных событий (подключение/отключение источника, ошибки)
- Счётчик обработанных тиков по каждой бирже
- Периодический health-check статуса в лог (каждые 30 сек)
- События `OnError` для критических ошибок

## Технологический стек

- **.NET 8** — основная платформа
- **Entity Framework Core 8** — ORM для работы с БД
- **PostgreSQL 16** — база данных
- **Docker / Docker Compose** — контейнеризация БД и pgAdmin
- **Polly 8** — политики повторных попыток (retry с exponential backoff)
- **Newtonsoft.Json** — парсинг JSON сообщений бирж
- **Npgsql** — драйвер PostgreSQL для .NET
- **WebSocket (System.Net.WebSockets)** — протокол для реального времени

## Структура проекта

```
MarketDataCollector/
├── src/
│   ├── MarketDataCollector.Core/              # Интерфейсы, базовые классы, конфигурация
│   │   ├── Clients/BaseWebSocketClient.cs     # Базовый WebSocket клиент с retry и автовосстановлением
│   │   ├── Configuration/ExchangeConfig.cs    # Модели конфигурации бирж
│   │   └── Interfaces/                        # Все интерфейсы системы
│   ├── MarketDataCollector.Domain/            # Сущности домена, доменные интерфейсы
│   │   ├── Entities/RawTick.cs                # Сырой тик
│   │   ├── Entities/AggregatedData.cs         # Агрегированные данные (свечи)
│   │   ├── Entities/ConnectionLog.cs          # Лог подключений
│   │   └── Interfaces/ITimeService.ts         # Абстракция времени
│   ├── MarketDataCollector.Infrastructure/    # Реализации (репозитории, клиенты, фабрики)
│   │   ├── Clients/BinanceWebSocketClient.cs  # Клиент Binance
│   │   ├── Data/MarketDataDbContext.cs        # EF Core DbContext
│   │   ├── Factories/WebSocketClientFactory.cs# Фабрика WebSocket клиентов
│   │   ├── Repositories/RawTickRepository.cs  # Репозиторий тиков
│   │   └── Services/SystemTimeService.cs     # Реализация ITimeService
│   ├── MarketDataCollector.Application/       # Бизнес-логика, сервисы
│   │   ├── Services/MarketDataProcessor.cs    # Процессор тиков (Channel + batch)
│   │   ├── Services/DataStorageService.cs     # Сервис хранения данных
│   │   └── Services/MonitoringService.cs      # Сервис мониторинга
│   └── MarketDataCollector.Workers/           # Фоновый сервис сбора данных
│       └── MarketDataCollector.Worker/
│           ├── Program.cs                     # Точка входа, регистрация DI
│           ├── Worker.cs                      # BackgroundService с health-check
│           ├── appsettings.json               # Конфигурация
│           └── appsettings.Development.json   # Конфигурация для разработки
├── tests/                                     # Тестовые проекты
│   ├── BinanceTick/                           # Консольный монитор Binance
│   └── KrakenTick/                            # Консольный монитор Kraken
├── docker/                                    # Docker конфигурации
│   ├── docker-compose.yml
│   └── init.sql
├── plans/                                     # Планы рефакторинга
└── docs/                                      # Документация
```

## Архитектура

### Слоистая архитектура с чистыми зависимостями

```
┌─────────────────────────────────────────────────────────┐
│  MarketDataCollector.Worker (BackgroundService)         │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────┐   │
│  │  Worker     │  │  Health-Check    │  │  Cleanup  │   │
│  └──────┬──────┘  └──────────────────┘  └───────────┘   │
└─────────┼───────────────────────────────────────────────┘
          │ использует
┌─────────▼───────────────────────────────────────────────┐
│  MarketDataCollector.Application                        │
│  ┌───────────────────┐  ┌────────────────────────────┐  │
│  │MarketDataProcessor│  │ MonitoringService          │  │
│  │(Channel + Batch)  │  │ (Counters + Status)        │  │
│  └───────────────────┘  └────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────┘
          │ реализует интерфейсы
┌─────────▼───────────────────────────────────────────────┐
│  MarketDataCollector.Core                               │
│  ┌───────────────────┐  ┌────────────────────────────┐  │
│  │BaseWebSocketClient│  │ Interfaces                 │  │
│  │(Polly retry,      │  │ (IExchangeWebSocketClient, │  │
│  │ auto-reconnect)   │  │  IMarketDataProcessor,     │  │
│  │                   │  │  IWebSocketClientFactory)  │  │
│  └───────────────────┘  └────────────────────────────┘  │
└─────────┬───────────────────────────────────────────────┘
          │ ссылается на
┌─────────▼───────────────────────────────────────────────┐
│  MarketDataCollector.Domain                             │
│  ┌──────────┐  ┌───────────────┐  ┌──────────────────┐  │
│  │ RawTick  │  │ AggregatedData│  │ ConnectionLog    │  │
│  └──────────┘  └───────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────┘
          ▲ реализуется
┌─────────┴───────────────────────────────────────────────┐
│  MarketDataCollector.Infrastructure                     │
│  ┌────────────────────┐  ┌───────────────────────────┐  │
│  │ BinanceWebSocket   │  │ RawTickRepository         │  │
│  │ Client             │  │ (EF Core)                 │  │
│  └────────────────────┘  └───────────────────────────┘  │
│  ┌────────────────────┐  ┌───────────────────────────┐  │
│  │ WebSocketClient    │  │ MarketDataDbContext       │  │
│  │ Factory            │  │                           │  │
│  └────────────────────┘  └───────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Поток данных

```
Binance WebSocket → BinanceWebSocketClient → ProcessMessageAsync()
    → IMarketDataProcessor.ProcessTickAsync() → Channel<TickData>
        → MarketDataProcessor (batch loop) → дедупликация
            → IRawTickRepository.AddRangeAsync() → PostgreSQL
```

### Принципы SOLID
1. **Single Responsibility** — каждый класс имеет одну ответственность
2. **Open/Closed** — новые биржи добавляются через наследование от `BaseWebSocketClient`
3. **Liskov Substitution** — клиенты бирж взаимозаменяемы через `IExchangeWebSocketClient`
4. **Interface Segregation** — интерфейсы разделены по функциональности
5. **Dependency Inversion** — все зависимости через интерфейсы и DI-контейнер

### Паттерны проектирования
- **Repository** — для доступа к данным (`IRawTickRepository`, `IRepository<T>`)
- **Factory** — для создания WebSocket клиентов (`WebSocketClientFactory`)
- **Observer** — события WebSocket (`MessageReceived`, `Connected`, `Disconnected`, `ErrorOccurred`)
- **Strategy** — различные форматы данных бирж (через наследование `BaseWebSocketClient`)
- **Channel** — асинхронная очередь с backpressure для батчевой обработки

## Быстрый старт

### Предварительные требования

1. **.NET 8 SDK** — [скачать](https://dotnet.microsoft.com/download/dotnet/8.0)
2. **Docker Desktop** — [скачать](https://www.docker.com/products/docker-desktop)

### Шаг 1: Запуск PostgreSQL в Docker

```bash
cd docker
docker-compose up -d
```

Проверьте, что контейнеры запущены:
```bash
docker ps
```

Доступ к сервисам:
- **PostgreSQL**: `localhost:5433`
  - База: `MarketDataDb`
  - Пользователь: `marketdata_user`
  - Пароль: `StrongPassword123!`
- **PgAdmin**: `http://localhost:5050`
  - Email: `admin@marketdata.local`
  - Пароль: `admin123`

### Шаг 2: Сборка решения

```bash
dotnet restore
dotnet build
```

### Шаг 3: Настройка конфигурации

Файл `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json`:

```json
{
  "ConnectionStrings": {
    "MarketDataDb": "Host=localhost;Port=5433;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!"
  },
  "MarketDataProcessor": {
    "BatchSize": 100,
    "ChannelCapacity": 10000
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
  }
}
```

**Параметры конфигурации:**
- `ConnectionStrings.MarketDataDb` — строка подключения к PostgreSQL
- `MarketDataProcessor.BatchSize` — размер батча для записи в БД (по умолчанию 100)
- `MarketDataProcessor.ChannelCapacity` — ёмкость канала сообщений (по умолчанию 10000)
- `ExchangeOptions.Exchanges` — массив бирж с шаблонами URL
- `ExchangeOptions.Readers` — массив ридеров (пара биржа + символ для подписки)

### Шаг 4: Запуск воркера сбора данных

```bash
cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
dotnet run
```

**Ожидаемый вывод при успешном запуске:**

```
info: MarketDataCollector.Worker.Worker[0]
      Worker starting...
info: MarketDataCollector.Worker.Worker[0]
      Starting 5 WebSocket clients...
info: MarketDataCollector.Worker.Worker[0]
      Health-check: 5 connected, 0 disconnected
```

Воркер работает непрерывно, автоматически переподключаясь при обрывах соединения.

## Добавление новой биржи

1. Создайте класс клиента, наследуемый от `BaseWebSocketClient`:
```csharp
public class NewExchangeWebSocketClient : BaseWebSocketClient
{
    private readonly IMarketDataProcessor _dataProcessor;

    public NewExchangeWebSocketClient(
        string webSocketUrl, string exchangeName, string symbol,
        IMarketDataProcessor dataProcessor)
        : base(webSocketUrl, exchangeName, symbol)
    {
        _dataProcessor = dataProcessor;
    }

    protected override async Task ProcessMessageAsync(string message)
    {
        // Парсинг специфичного формата биржи
        // Вызов _dataProcessor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);
    }

    protected override async Task SubscribeToTickerAsync(CancellationToken cancellationToken)
    {
        // Отправка сообщения подписки, если требуется
    }
}
```

2. Добавьте фабричный метод в `WebSocketClientFactory`
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

## Мониторинг и логирование

### Логи
- Уровни: Debug, Information, Warning, Error, Critical
- Выход: Console (настраивается через `appsettings.json`)
- События: подключение, отключение, ошибки, статистика, health-check

### Метрики (через MonitoringService)
- Количество обработанных тиков по каждой бирже
- Статус подключений (Connected / Disconnected / Error)
- Общее количество обработанных тиков

### Health-check
- Периодическая проверка каждые 30 секунд
- Автоматический перезапуск отключённых клиентов
- Логирование статуса: подключено/отключено

## Разработка

### Запуск тестовых мониторов

Тестовые проекты — консольные приложения для мониторинга тиков без записи в БД:

```bash
# Монитор Binance
cd tests/BinanceTick
dotnet run

# Монитор Kraken
cd tests/KrakenTick
dotnet run
```

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
3. Проверьте логику проверки дубликатов в `MarketDataProcessor`

### Проблема: Воркер падает с ошибкой
**Решение:**
1. Проверьте логи на наличие `Critical` ошибок
2. Убедитесь, что процессор не завершился с ошибкой (`OnError` event)
3. Перезапустите воркер — он восстановит соединения

## Лицензия

MIT License

## Контакты

Для вопросов и предложений создавайте issue в репозитории проекта.

telegram: @Omsk1984

---

*Последнее обновление: май 2026*
