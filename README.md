# Market Data Collector System

Система сбора, обработки и хранения ценовых данных с нескольких бирж в реальном времени.

## Описание

Система предназначена для сбора тиковых данных (котировок) с бирж через WebSocket соединения, нормализации данных, удаления дубликатов и сохранения в базу данных PostgreSQL. Поддерживает параллельную работу с 2-3 источниками данных, обработку до 50-100 тиков/сек.

## Функционал

### 1. Сбор данных
- WebSocket клиенты для подключения к биржам (Binance, Coinbase, Kraken и др.)
- Поддержка нескольких одновременных подключений
- Автоматическое переподключение при обрывах соединения

### 2. Обработка потока данных
- Нормализация к единому формату (тикер, цена, объем, timestamp)
- Удаление дубликатов на основе уникального ключа (тикер + биржа + timestamp)
- Асинхронная обработка через очередь

### 3. Хранение в БД
- Сохранение сырых тиков в PostgreSQL
- Поддержка агрегированных данных (опционально)
- Логирование подключений и ошибок

### 4. Мониторинг
- Логирование основных событий (подключение/отключение источника, ошибки)
- Счетчик обработанных тиков
- Периодический вывод статуса в консоль

## Технологический стек

- **.NET 8** - основная платформа
- **Entity Framework Core 8** - ORM для работы с БД
- **PostgreSQL 16** - база данных
- **Docker** - контейнеризация БД
- **WebSocket** - протокол для реального времени
- **Npgsql** - драйвер PostgreSQL для .NET

## Структура проекта

```
MarketDataCollector/
├── src/
│   ├── MarketDataCollector.Core/          # Интерфейсы, базовые классы
│   ├── MarketDataCollector.Domain/        # Сущности домена
│   ├── MarketDataCollector.Infrastructure/# Реализации (репозитории, клиенты)
│   ├── MarketDataCollector.Application/   # Бизнес-логика, сервисы
│   ├── MarketDataCollector.Workers/       # Фоновый сервис сбора данных
│   └── MarketDataCollector.WebApi/        # Веб-API (опционально)
├── tests/                                 # Тесты
├── docker/                                # Docker конфигурации
│   ├── docker-compose.yml
│   └── init.sql
├── config/                                # Конфигурационные файлы
└── docs/                                  # Документация
```

## Быстрый старт

### Предварительные требования

1. **.NET 8 SDK** - [скачать](https://dotnet.microsoft.com/download/dotnet/8.0)
2. **Docker Desktop** - [скачать](https://www.docker.com/products/docker-desktop)
3. **PostgreSQL клиент** (опционально) - для просмотра данных

### Шаг 1: Запуск PostgreSQL в Docker

```bash
cd docker
docker-compose up -d
```

Проверьте, что контейнеры запущены:
```bash
docker ps
```

Доступ к базам данных:
- **PostgreSQL**: `localhost:5432`
  - База: `MarketDataDb`
  - Пользователь: `marketdata_user`
  - Пароль: `StrongPassword123!`
- **PgAdmin**: `http://localhost:5050`
  - Email: `admin@marketdata.local`
  - Пароль: `admin123`

### Шаг 2: Настройка подключения к БД

Отредактируйте файл `appsettings.json` в проекте WebApi:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!"
  }
}
```

### Шаг 3: Применение миграций EF Core

```bash
cd src/MarketDataCollector.WebApi
dotnet ef database update
```

Или используйте скрипт инициализации в Docker (уже выполнен при первом запуске).

### Шаг 4: Сборка решения

```bash
# Восстановление зависимостей
dotnet restore

# Сборка решения
dotnet build
```

### Шаг 5: Запуск воркера сбора данных

Воркер (`MarketDataCollector.Worker`) — это фоновый сервис, который подключается к биржам через WebSocket и сохраняет тиковые данные в БД.

**Важно:** Перед запуском воркера убедитесь, что:
1. PostgreSQL запущен (см. Шаг 1)
2. Миграции применены (см. Шаг 3)

**Настройка воркера:**

Отредактируйте файл `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json`:

```json
{
  "ConnectionStrings": {
    "MarketDataDb": "Host=localhost;Port=5432;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!"
  },
  "Exchanges": [
    {
      "ExchangeName": "binance",
      "WebSocketUrl": "wss://stream.binance.com:9443/ws/btcusdt@trade"
    }
  ]
}
```

**Параметры конфигурации:**
- `ConnectionStrings.MarketDataDb` — строка подключения к PostgreSQL
- `Exchanges` — массив бирж для подключения:
  - `ExchangeName` — название биржи (например, `binance`, `coinbase`)
  - `WebSocketUrl` — URL WebSocket потока данных

**Примеры URL для различных бирж:**

| Биржа | URL для торгов по BTC/USDT |
|-------|---------------------------|
| Binance | `wss://stream.binance.com:9443/ws/btcusdt@trade` |
| Binance (несколько пар) | `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade` |

**Запуск воркера:**

```bash
cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
dotnet run
```
убить зависший сервис
```ps
tasklist | findstr MarketDataCollector
taskkill /PID 19356 /F
---

**Ожидаемый вывод при успешном запуске:**

```
info: MarketDataCollector.Worker.Worker[0]
      Worker starting...
info: MarketDataCollector.Worker.Worker[0]
      Using exchange: binance
info: MarketDataCollector.Worker.Worker[0]
      Connecting to wss://stream.binance.com:9443/ws/btcusdt@trade...
info: MarketDataCollector.Worker.Worker[0]
      Connected to WebSocket
info: MarketDataCollector.Worker.Worker[0]
      Waiting for first tick...
info: MarketDataCollector.Worker.Worker[0]
      Processing tick: Ticker=BTCUSDT, Price=XXXXX.XX, Volume=X.XXXX, Timestamp=...
info: MarketDataCollector.Worker.Worker[0]
      Tick processed and saved to database.
```

> **Примечание:** Текущая реализация воркера обрабатывает первый полученный тик и завершает работу. Для непрерывного сбора данных потребуется доработка цикла обработки сообщений.

### Шаг 6 (опционально): Запуск WebApi

```bash
cd src/MarketDataCollector.WebApi
dotnet run
```

## Конфигурация

### Настройка бирж

В `appsettings.json` добавьте секцию `Exchanges`:

```json
{
  "Exchanges": {
    "Binance": {
      "WebSocketUrl": "wss://stream.binance.com:9443/ws",
      "Enabled": true,
      "Symbols": ["btcusdt", "ethusdt", "bnbusdt"]
    },
    "Coinbase": {
      "WebSocketUrl": "wss://ws-feed.exchange.coinbase.com",
      "Enabled": false,
      "Symbols": ["BTC-USD", "ETH-USD"]
    }
  }
}
```

### Настройка логирования

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft": "Warning",
      "Microsoft.Hosting.Lifetime": "Information",
      "MarketDataCollector": "Debug"
    }
  }
}
```

## Архитектура

### Принципы SOLID
1. **Single Responsibility** - каждый класс имеет одну ответственность
2. **Open/Closed** - классы открыты для расширения, закрыты для изменений
3. **Liskov Substitution** - наследники могут заменять родительские классы
4. **Interface Segregation** - интерфейсы разделены по функциональности
5. **Dependency Inversion** - зависимости через интерфейсы

### Паттерны проектирования
- **Repository** - для доступа к данным
- **Strategy** - для различных форматов данных бирж
- **Observer** - для обработки событий WebSocket
- **Factory** - для создания клиентов бирж
- **Singleton** - для сервисов мониторинга

## Производительность

### Целевые показатели
- Обработка: 50-100 тиков/сек суммарно
- Задержка: < 100 мс от получения до сохранения
- Параллельная работа: 2-3 WebSocket подключения одновременно

### Оптимизации
- Асинхронная обработка через ConcurrentQueue
- Batch-вставка в БД
- Connection pooling для PostgreSQL
- In-memory кэширование для проверки дубликатов

## Мониторинг и логирование

### Логи
- Уровни: Debug, Information, Warning, Error
- Выходы: Console, File, Seq (опционально)
- События: подключение, отключение, ошибки, статистика

### Метрики
- Количество обработанных тиков
- Статус подключений к биржам
- Время обработки
- Ошибки и исключения

### API для мониторинга (WebApi)
```
GET /api/health          # Проверка здоровья системы
GET /api/status          # Статус подключений
GET /api/stats           # Статистика обработки
GET /api/ticks/count     # Количество тиков в БД
```

## Разработка

### Добавление новой биржи

1. Создайте класс клиента, наследуемый от `BaseWebSocketClient`
2. Реализуйте парсинг формата данных биржи
3. Добавьте конфигурацию в `appsettings.json`
4. Зарегистрируйте в DI-контейнере

### Пример клиента для новой биржи

```csharp
public class NewExchangeWebSocketClient : BaseWebSocketClient
{
    public NewExchangeWebSocketClient(IMarketDataProcessor processor) 
        : base("wss://api.newexchange.com/ws", "NewExchange")
    {
        // Инициализация
    }

    protected override async Task ProcessMessageAsync(string message)
    {
        // Парсинг специфичного формата
        // Вызов processor.ProcessTickAsync(...)
    }
}
```

## Тестирование

### Запуск тестов

```bash
dotnet test
```

### Типы тестов
- **Unit tests** - тестирование отдельных компонентов
- **Integration tests** - тестирование с реальной БД
- **Load tests** - тестирование производительности

## Развертывание

### Docker
```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 80

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["MarketDataCollector.sln", "."]
# ... остальная конфигурация
```

### Kubernetes
Пример манифестов в `k8s/` директории.

## Безопасность

### Рекомендации
1. Используйте секреты для паролей БД
2. Настройте SSL/TLS для WebSocket соединений
3. Ограничьте доступ к API
4. Регулярно обновляйте зависимости

### Переменные окружения
```bash
export DB_PASSWORD=your_secure_password
export API_KEY=your_exchange_api_key
```

## Устранение неполадок

### Проблема: Не подключается к бирже
**Решение:**
1. Проверьте интернет-соединение
2. Убедитесь, что URL биржи корректен
3. Проверьте наличие блокировок фаерволом
4. Проверьте логи на наличие ошибок

### Проблема: Медленная обработка
**Решение:**
1. Увеличьте размер batch-вставок
2. Оптимизируйте запросы к БД
3. Проверьте нагрузку на сеть и БД
4. Рассмотрите горизонтальное масштабирование

### Проблема: Дубликаты в БД
**Решение:**
1. Проверьте уникальный индекс в БД
2. Убедитесь в корректности timestamp
3. Проверьте логику проверки дубликатов

## Лицензия

MIT License

## Контакты

Для вопросов и предложений создавайте issue в репозитории проекта.

telegram: @Omsk1984

---

*Последнее обновление: май 2026*