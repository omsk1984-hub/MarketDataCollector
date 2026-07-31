# Market Data Collector System

Система сбора, обработки и хранения ценовых данных с криптобирж в реальном времени.

> **Результаты нагрузочного тестирования:** FakeTickServer (~25 000 msg/s, 3 символа) + **Single Consumer** (по умолчанию) + Binary COPY protocol. Sequential-режим (batch=2500) даёт ~10 700 ticks/sec, что покрывает текущую нагрузку (~19K msg/s). Single Consumers (per-ticker routing) даёт до ~19–24K processed ticks/sec при входящем потоке ~25 000 msg/s при непрерывной нагрузке. Канал (ChannelCapacity=150000) утилизирует backlog при простое генератора. Потери уникальных данных при graceful shutdown — **0** (благодаря `_internalCts`).

## Описание

Система предназначена для непрерывного сбора тиковых данных (сделок) с криптобирж через WebSocket соединения, нормализации данных, удаления дубликатов и сохранения в базу данных PostgreSQL. Поддерживает параллельную работу с несколькими источниками данных и символами. Архитектура построена на принципах SOLID с чистыми зависимостями и делегированием ответственности специализированным компонентам.

**Ключевые особенности:**
- **Single Consumer (по умолчанию)** — ровно 1 consumer + 1 writer, `SingleReader=true`, полностью исключает deadlock'и (40P01) и lock contention
- **Multiple Consumers Mode** — опционально N параллельных consumer'ов с per-ticker routing (disjoint наборы тикеров), для throughput > 25K ticks/sec
- **Async Writer** — отдельный канал для записи батчей, Collector не блокируется на записи
- **Adaptive Batch Size** — автоматическая подстройка размера батча под backlog и скорость записи
- **MinPartialBatchSize** — защита от микробатчей при flush по таймеру
- **DeduplicationCache** — in-memory FIFO-кэш для ранней дедупликации до БД (10 000 записей)
- **CounterBatcher** — батчевый сбор per-message метрик (Interlocked.Increment без lock) для снижения lock contention OpenTelemetry
- **TickAggregator** — агрегация OHLCV-свечей с опциональной публикацией через Kafka
- **SlidingWindowCounter** — lock-free счётчик RPS со скользящим окном (60 секунд)
- **OpenTelemetry / Prometheus** — метрики, трейсинг и структурированные логи

## Функционал

### 1. Сбор данных
- WebSocket клиенты для подключения к биржам (Binance — реализована, остальные — через расширение)
- Поддержка нескольких одновременных подключений (разные символы)
- Автоматическое переподключение при обрывах соединения (экспоненциальный backoff через [`IReconnectStrategy`](src/MarketDataCollector.Core/Interfaces/IReconnectStrategy.cs))
- Фоновый health-check каждые 10 секунд с перезапуском отключённых клиентов
- Делегированная архитектура: [`IWebSocketConnectionManager`](src/MarketDataCollector.Core/Interfaces/IWebSocketConnectionManager.cs) (управление соединением), [`IWebSocketMessageReceiver`](src/MarketDataCollector.Core/Interfaces/IWebSocketMessageReceiver.cs) (цикл приёма сообщений), [`ISubscriptionManager`](src/MarketDataCollector.Core/Interfaces/ISubscriptionManager.cs) (подписка), [`IReconnectStrategy`](src/MarketDataCollector.Core/Interfaces/IReconnectStrategy.cs) (переподключение)
- **Staggered startup** — клиенты запускаются с интервалом 2с, чтобы избежать стартового пика backlog и дропов

### 2. Обработка потока данных
- Нормализация к единому формату через [`TickData`](src/MarketDataCollector.Domain/Entities/TickData.cs) (value-type `readonly record struct` — иммутабельна, минимальные аллокации)
- **Трёхуровневая дедупликация**:
  1. [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) — in-memory FIFO (10 000 записей, batch-эвикция 10%)
  2. `GroupBy` в памяти `(Ticker, Exchange, Timestamp)` — внутри батча
  3. `ON CONFLICT DO NOTHING` — глобально, через unique-индекс БД
- **Single Consumer Mode** (по умолчанию): ровно 1 consumer + 1 writer, Channel с `SingleReader=true`. Полностью исключает deadlock'и (40P01), снижает GC-давление и lock contention. Sequential batch=2500 даёт ~10 700 ticks/sec.
- **Multiple Consumers Mode** (`UseSingleConsumer=false`): N параллельных consumer'ов, каждый получает disjoint набор тикеров через per-ticker routing (hash ticker'а) → B-tree страницы unique-индекса не пересекаются → deadlock'и невозможны. Полезен при throughput > 25K ticks/sec. `ConsumerCount=0` → авто `Math.Clamp(CPU/2, 1, 4)`.
- **Async Writer** — Collector отправляет батчи Writer'у через отдельный `Channel<CollectedBatch>` (`BatchChannelCapacity`, FullMode=Wait → backpressure), Writer выполняет запись в БД. Это предотвращает блокировку Collector'а на записи.
- **Adaptive Batch Size** — автоматически подстраивает размер батча под backlog (`MinBatchSize`–`MaxBatchSize`, линейная интерполяция между `BacklogLowThreshold` и `BacklogHighThreshold`), плюс снижение на 20% при медленной записи (`WriteDurationWarningMs`)
- **MinPartialBatchSize** — минимальный размер частичного батча при flush по таймеру, предотвращает микробатчи
- **Bulk insert** через Binary COPY protocol (Npgsql) — в 10-100x быстрее `AddRangeAsync`
- Обработка критических ошибок с остановкой Worker для внешнего перезапуска (Docker/K8s)

### 3. Хранение в БД
- Сохранение сырых тиков в PostgreSQL через **Binary COPY protocol** (Npgsql) + temp table + `INSERT ON CONFLICT DO NOTHING`
- Уникальный индекс `(Ticker, Exchange, Timestamp)` — финальная защита от дубликатов на уровне БД
- **Deadlock-free** параллельная запись: per-ticker routing гарантирует непересекающиеся B-tree страницы
- Retry-логика (5 попыток, exponential backoff + jitter) как safety net
- **Агрегированные данные (OHLCV-свечи):** запись через [`TickAggregator`](src/MarketDataCollector.Application/Services/TickAggregator.cs) → Kafka (опционально) → [`KafkaCandleConsumerService`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs) или напрямую в БД. Настраиваемый интервал свечи (1-3600с), буферизация через `Channel<TickData>` с `DropOldest`
- Логирование подключений (сущность [`ConnectionLog`](src/MarketDataCollector.Domain/Entities/ConnectionLog.cs)) — fire-and-forget запись через [`MonitoringService`](src/MarketDataCollector.Application/Services/MonitoringService.cs)

### 4. Мониторинг и Observability

Система предоставляет три канала observability:

#### OpenTelemetry метрики (Prometheus + Aspire Dashboard)
Централизованный источник метрик — [`MarketDataTelemetry`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs):

Per-message счётчики (`ticks.incoming`, `ticks.dropped`, `ws.messages.received`) инкрементируются в hot path через [`CounterBatcher`](src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs) (Interlocked.Increment без lock и аллокаций), а реальный `Counter.Add` выносится один раз за батч через `FlushMetricBatchers()`. Это снижает внутренний lock contention OpenTelemetry. Имена/теги/единицы метрик при этом не меняются.

| Метрика | Тип | Описание |
|---------|-----|----------|
| `ws.messages.received` | Counter | Сообщения от WebSocket (теги: exchange, symbol) |
| `ticks.incoming` | Counter | Тики на входе в ProcessTickAsync |
| `ticks.received` | Counter | Тики, извлечённые из Channel в батч |
| `ticks.processed` | Counter | Тики, успешно записанные в БД |
| `ticks.dropped` | Counter | Тики, дропнутые каналом (TryWrite=false) |
| `ticks.dropped.silently` | Counter | Оценка дропов через DropOldest |
| `ws.active_connections` | UpDownCounter | Активные WebSocket соединения |
| `processor.channel.fill` | Histogram | Заполненность Channel |
| `processor.channel.backlog` | UpDownCounter | Backlog канала (incoming - received) |
| `processor.channel.fill_level` | UpDownCounter | Текущие тики в канале по channel_index |
| `ticks.batch.size` | Histogram | Распределение размера батча |
| `ticks.batch.adaptive_size` | Histogram | Адаптивный batch size |
| `ticks.batch.write.duration` | Histogram | Длительность записи батча (ms) |
| `processor.batch_channel.fill` | Histogram | Заполненность batch channel |
| `exceptions_total` | Counter | Исключения по типам (exception_type, sql_state) |

**Метрики экспортируются:**
- **Prometheus** — `/metrics` endpoint на порту 5010
- **OTLP** — Aspire Dashboard (gRPC: `localhost:18889`)

#### Health-check (в [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs))
- Периодическая проверка каждые 10 секунд
- RPS метрики: входящие/обработанные (через [`SlidingWindowCounter`](src/MarketDataCollector.Core/Utilities/SlidingWindowCounter.cs))
- Per-channel fill percentages — заполненность каждого канала
- Backlog и оценка дропов
- Автоматический перезапуск отключённых клиентов
- Остановка Worker при критической ошибке
- **HTTP endpoint `/health`** — проверка PostgreSQL и Kafka (через [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs))

#### Трейсинг (OpenTelemetry Tracing)
- **EF Core** — автоматический трейсинг всех запросов к БД
- **Business operations** — через `MarketDataTelemetry.ActivitySource`
- Экспорт через OTLP в Aspire Dashboard

## Технологический стек

- **.NET 8** — основная платформа
- **Entity Framework Core 8** — ORM для работы с БД
- **PostgreSQL 16** — база данных
- **Apache Kafka (KRaft mode, v3.9.0)** — брокер сообщений для развязки компонентов
- **Confluent.Kafka** — .NET клиент для Kafka (Idempotent Producer, Snappy compression, acks=all)
- **Docker / Docker Compose** — контейнеризация (PostgreSQL, Kafka, Kafdrop, Prometheus, Aspire Dashboard)
- **OpenTelemetry** — метрики, трейсинг, логи (экспорт через OTLP + Prometheus)
- **Prometheus** — сбор и хранение метрик (retention 7 дней)
- **Aspire Dashboard** — визуализация метрик, трейсов и логов
- **Polly 8** — политики повторных попыток (референс в проекте; фактическая стратегия — собственная реализация [`ExponentialReconnectStrategy`](src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs))
- **Newtonsoft.Json** — парсинг JSON сообщений бирж
- **Npgsql** — драйвер PostgreSQL для .NET (Binary COPY protocol)
- **WebSocket (`System.Net.WebSockets`)** — протокол для реального времени
- **xUnit + Moq + FluentAssertions** — модульное тестирование
- **Testcontainers** — интеграционные тесты с реальным Kafka/PostgreSQL в Docker

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
│   │   │   ├── MarketDataProcessorOptions.cs  # Параметры процессора
│   │   │   ├── TickAggregatorOptions.cs       # Параметры агрегации OHLCV-свечей
│   │   │   └── KafkaOptions.cs                # Параметры Kafka
│   │   ├── Interfaces/                        # Все интерфейсы системы
│   │   │   ├── IExchangeWebSocketClient.cs    # WebSocket клиент биржи
│   │   │   ├── IWebSocketClient.cs            # Базовый WebSocket клиент
│   │   │   ├── IClientWebSocket.cs            # Абстракция ClientWebSocket
│   │   │   ├── IWebSocketConnectionManager.cs # Менеджер соединения
│   │   │   ├── IWebSocketMessageReceiver.cs   # Приёмник сообщений
│   │   │   ├── IReconnectStrategy.cs          # Стратегия переподключения
│   │   │   ├── ISubscriptionManager.cs        # Менеджер подписки
│   │   │   ├── IWebSocketClientFactory.cs     # Фабрика клиентов
│   │   │   ├── IMarketDataProcessor.cs        # Процессор рыночных данных
│   │   │   ├── IMonitoringService.cs          # Сервис мониторинга
│   │   │   ├── ITickAggregator.cs             # Агрегатор OHLCV-свечей
│   │   │   ├── IRawTickRepository.cs          # Репозиторий тиков
│   │   │   ├── IConnectionLogRepository.cs    # Репозиторий логов подключений
│   │   │   ├── IAggregatedDataRepository.cs   # Репозиторий агрегированных данных
│   │   │   └── IRepository.cs                 # Базовый репозиторий
│   │   ├── Telemetry/
│   │   │   └── MarketDataTelemetry.cs         # OpenTelemetry метрики (Meter + ActivitySource)
│   │   └── Utilities/
│   │       └── SlidingWindowCounter.cs        # Lock-free счётчик RPS (60s окно)
│   ├── MarketDataCollector.Domain/            # Сущности домена, доменные интерфейсы
│   │   ├── Entities/
│   │   │   ├── RawTick.cs                     # Сырой тик
│   │   │   ├── TickData.cs                    # Value-type record struct для передачи тиков
│   │   │   ├── AggregatedData.cs              # Агрегированные данные (OHLCV-свечи)
│   │   │   └── ConnectionLog.cs               # Лог подключений
│   │   ├── Interfaces/
│   │   │   └── ITimeService.cs                # Абстракция времени
│   │   └── Utilities/
│   │       └── DecimalHelper.cs              # Хелперы для decimal
│   ├── MarketDataCollector.Infrastructure/    # Реализации (репозитории, клиенты, фабрики)
│   │   ├── Clients/BinanceWebSocketClient.cs  # Клиент Binance
│   │   ├── Data/MarketDataDbContext.cs        # EF Core DbContext
│   │   ├── Factories/WebSocketClientFactory.cs# Фабрика WebSocket клиентов (двухфазная инициализация)
│   │   ├── Kafka/
│   │   │   ├── IKafkaProducer.cs              # Generic Kafka producer interface
│   │   │   ├── KafkaProducer.cs               # Базовая реализация (Confluent.Kafka)
│   │   │   ├── KafkaCandleProducer.cs         # Producer для OHLCV-свечей
│   │   │   └── KafkaCandleConsumerService.cs  # Consumer свечей (Kafka → PostgreSQL)
│   │   ├── Repositories/
│   │   │   ├── RawTickRepository.cs           # Репозиторий тиков (Binary COPY)
│   │   │   ├── AggregatedDataRepository.cs    # Репозиторий агрегированных данных
│   │   │   └── ConnectionLogRepository.cs     # Репозиторий логов подключений
│   │   └── Services/SystemTimeService.cs      # Реализация ITimeService
│   ├── MarketDataCollector.Application/       # Бизнес-логика, сервисы
│   │   └── Services/
│   │       ├── MarketDataProcessor.cs         # Процессор тиков (Channel + batch + дедупликация)
│   │       ├── MarketDataProcessor.Logging.cs # Логирование процессора (partial class)
│   │       ├── DeduplicationCache.cs          # In-memory FIFO кэш дедупликации
│   │       ├── TickAggregator.cs              # Агрегатор OHLCV-свечей (Channel + Kafka/DB)
│   │       └── MonitoringService.cs           # Сервис мониторинга (счётчики + ConnectionLog)
│   └── MarketDataCollector.Workers/           # Фоновый сервис сбора данных
│       └── MarketDataCollector.Worker/
│           ├── Program.cs                     # Точка входа, DI, OpenTelemetry, /health, /metrics
│           ├── Worker.cs                      # BackgroundService с health-check
│           ├── appsettings.json               # Конфигурация
│           ├── appsettings.Development.json   # Конфигурация для разработки
│           └── Properties/launchSettings.json
├── tests/                                     # Тестовые проекты
│   ├── MarketDataCollector.Tests/             # xUnit модульные тесты (16+ файлов)
│   │   ├── Application/Services/
│   │   │   ├── MarketDataProcessorTests.cs
│   │   │   ├── MonitoringServiceTests.cs
│   │   │   ├── DeduplicationCacheTests.cs
│   │   │   └── TickAggregatorTests.cs
│   │   ├── Core/Clients/
│   │   │   ├── BaseWebSocketClientTests.cs
│   │   │   ├── ExponentialReconnectStrategyTests.cs
│   │   │   ├── SubscriptionManagerTests.cs
│   │   │   ├── WebSocketConnectionManagerTests.cs
│   │   │   └── WebSocketMessageReceiverTests.cs
│   │   ├── Core/Configuration/
│   │   ├── DomainUtilities/
│   │   │   └── DecimalHelperTests.cs
│   │   └── Infrastructure/
│   │       ├── Clients/BinanceWebSocketClientTests.cs
│   │       ├── Factories/WebSocketClientFactoryTests.cs
│   │       ├── Kafka/
│   │       │   ├── KafkaIntegrationTests.cs
│   │       │   └── KafkaRealConnectionTests.cs
│   │       └── Repositories/
│   │           ├── RawTickRepositoryTests.cs
│   │           └── RawTickRepositoryDeadlockTests.cs
│   ├── BinanceTick/                           # Консольный монитор Binance
│   ├── KrakenTick/                            # Консольный монитор Kraken
│   ├── FakeTickServer/                        # Симулятор биржи для нагрузочного тестирования
│   └── TickWriteBenchmark/                    # Бенчмарк производительности записи/чтения тиков
├── docker/                                    # Docker конфигурации
│   ├── docker-compose.yml                     # PostgreSQL 16 + Kafka KRaft + Kafdrop + Prometheus + Aspire Dashboard
│   ├── init.sql                               # Инициализация схемы БД
│   ├── init-partitioned.sql                   # Партиционирование таблицы RawTicks по дням
│   ├── kafka/
│   │   └── init-topics.sh                     # Скрипт создания топиков Kafka
│   ├── prometheus/
│   │   └── prometheus.yml                     # Конфигурация Prometheus (scrape /metrics)
│   └── migrations/                            # Директория для миграций БД
├── scripts/                                   # Скрипты сбора метрик и профилирования
│   ├── collect-counters.ps1                   # Сбор Prometheus-метрик в CSV
│   ├── collect-trace.ps1                      # dotnet-trace с allocation tracking
│   ├── collect-gcdump.ps1                     # dotnet-gcdump (2 снапшота)
│   ├── collect-all.ps1                        # Всё сразу: counters + trace + gcdump
│   ├── analyze_counters.ps1                   # Анализ CSV-файлов счётчиков
│   ├── run-batching-loadtest.ps1              # Нагрузочный тест батчинга
│   ├── common-functions.ps1                   # Общие функции для скриптов
│   └── vscode-terminal-init.ps1               # Инициализация терминала VS Code
├── config/                                    # Дополнительные конфигурации
├── plans/                                     # Планы рефакторинга и оптимизации
│   ├── counters-analysis-*.md                 # Анализ счётчиков производительности
│   ├── optimization-execution-plan.md         # План оптимизации производительности
│   ├── fix-deadlock-40p01-plan.md             # План исправления deadlock'ов PostgreSQL
│   └── ... (другие планы и analysis)
├── traces/                                    # Результаты профилирования (nettrace, gcdump, CSV)
├── metrics.ps1                                # Диспетчер сбора метрик/профилирования (counters/trace/gcdump/all)
├── run_all_metrics.ps1                        # Запуск полного профилирования (all)
├── old_counter.ps1                            # Устаревший сбор метрик через /metrics (совместимость)
├── run.ps1                                    # Сборка и запуск воркера
├── run_test.ps1                               # Запуск тестов
├── run_benchmark.ps1                          # Запуск TickWriteBenchmark
├── run_fake_server.ps1                        # Запуск FakeTickServer (нагрузочное тестирование)
├── read_baseline.json                         # Baseline READ-бенчмарка
└── read_partitioned.json                      # Partitioned READ-бенчмарка
```

## Архитектура

### Слоистая архитектура с делегированными компонентами

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Worker (BackgroundService)                                  │
│  ┌─────────────┐  ┌──────────────────┐  ┌───────────┐  ┌──────────────────────┐  │
│  │  Worker     │  │  Health-Check    │  │  Cleanup  │  │  GC Optimization     │  │
│  │             │  │  (10s interval)  │  │           │  │  LOH compaction 5min │  │
│  └──────┬──────┘  └──────────────────┘  └───────────┘  └──────────────────────┘  │
└─────────┼────────────────────────────────────────────────────────────────────────┘
          │ использует
┌─────────▼────────────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Infrastructure                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  WebSocketClientFactory (двухфазная инициализация)                        │    │
│  │  ┌────────────────────────────────────────────────────────────────────┐   │    │
│  │  │  BinanceWebSocketClient (IExchangeWebSocketClient)                  │   │    │
│  │  │  ┌────────────────┐ ┌───────────────┐                              │   │    │
│  │  │  │ ConnectionMgr  │ │ MessageRcvr   │                              │   │    │
│  │  │  ├────────────────┤ ├───────────────┤                              │   │    │
│  │  │  │ ReconnectStrat │ │ SubscriptionMgr│                              │   │    │
│  │  │  └────────────────┘ └───────────────┘                              │   │    │
│  │  └────────────────────────────────────────────────────────────────────┘   │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────┐  ┌───────────────────────────────────────────────────┐   │
│  │ RawTickRepository  │  │ MarketDataDbContext                               │   │
│  │ ConnectionLogRepo  │  │ (EF Core: RawTicks, ConnectionLogs,               │   │
│  │ AggregatedDataRepo │  │  AggregatedData)                                  │   │
│  └────────────────────┘  └───────────────────────────────────────────────────┘   │
│  ┌────────────────────┐  ┌───────────────────────────────────────────────────┐   │
│  │ SystemTimeService  │  │ Kafka: IKafkaProducer, KafkaCandleProducer        │   │
│  └────────────────────┘  │ KafkaCandleConsumerService (hosted service)        │   │
│                          └───────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
          ▲ реализует интерфейсы
┌─────────┴────────────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Core (Interfaces + BaseWebSocketClient)                     │
│  ┌──────────────────────────────────────────────────────────────────────────┐    │
│  │  IExchangeWebSocketClient ← BaseWebSocketClient                          │    │
│  │  IWebSocketConnectionManager ← WebSocketConnectionManager                │    │
│  │  IWebSocketMessageReceiver  ← WebSocketMessageReceiver                   │    │
│  │  IReconnectStrategy         ← ExponentialReconnectStrat                  │    │
│  │  ISubscriptionManager       ← SubscriptionManager                        │    │
│  │  IWebSocketClientFactory    ← (интерфейс фабрики)                         │    │
│  │  IMarketDataProcessor       ← (интерфейс процессора)                      │    │
│  │  IMonitoringService         ← (интерфейс мониторинга)                     │    │
│  │  ITickAggregator            ← (интерфейс агрегатора)                      │    │
│  │  IAggregatedDataRepository  ← (интерфейс репозитория свечей)              │    │
│  │  MarketDataTelemetry        ← OpenTelemetry Meter + ActivitySource        │    │
│  │  SlidingWindowCounter       ← Lock-free RPS counter                       │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────────┘
          │ использует
┌─────────▼────────────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Application                                                 │
│  ┌─────────────────────────┐  ┌────────────────────────────┐                      │
│  │ MarketDataProcessor     │  │ MonitoringService          │                      │
│  │ (Channel + Batch +      │  │ (Counters + Status +       │                      │
│  │  Async Writer +         │  │  ConnectionLog fire&forget)│                      │
│  │  Adaptive Batch Size +  │  └────────────────────────────┘                      │
│  │  DeduplicationCache)    │  ┌────────────────────────────┐                      │
│  └─────────────────────────┘  │ TickAggregator             │                      │
│  ┌─────────────────────────┐  │ (OHLCV Candles:           │                      │
│  │ DeduplicationCache      │  │  Channel → Kafka/DB)      │                      │
│  └─────────────────────────┘  └────────────────────────────┘                      │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
          ▲ реализует интерфейсы Domain
┌─────────┴────────────────────────────────────────────────────────────────────────┐
│  MarketDataCollector.Domain                                                      │
│  ┌──────────┐  ┌───────────────┐  ┌──────────────────┐  ┌──────────┐              │
│  │ RawTick  │  │ TickData      │  │ AggregatedData   │  │ ConnLog  │              │
│  │          │  │ (record struct)│  │ (OHLCV candle)   │  │          │              │
│  └──────────┘  └───────────────┘  └──────────────────┘  └──────────┘              │
│  ┌────────────────────┐  ┌──────────────────┐                                    │
│  │ ITimeService       │  │ DecimalHelper    │                                    │
│  └────────────────────┘  └──────────────────┘                                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Поток данных

```
Binance WebSocket
    ↓ (сырые JSON сообщения)
BinanceWebSocketClient.ProcessMessageAsync()
    ↓ (парсинг в TickData + OTel метрики: ws.messages.received)
IWebSocketMessageReceiver (цикл приёма)
    ↓ (вызов ProcessMessageAsync)
IMarketDataProcessor.ProcessTickAsync()
    ↓ (per-ticker routing: hash(ticker) % consumerCount; в Single Consumer = 1)
    ↓ (+ OTel: ticks.incoming, ticks.dropped при переполнении)
Channel<TickData> (SingleReader=true, DropOldest; при Multiple Consumers — N каналов)
    ↓
Collector (Consumer: читает канал)
    ↓ (накопление батча, adaptive batch size 2500-5000)
    ↓ (+ DeduplicationCache — ранняя дедупликация)
    ↓ (OTel: ticks.received, channel fill level, backlog)
Отправка батча в Async Writer через Channel<CollectedBatch>
    ↓
Async Writer (один, пишет в БД)
    ↓ (дедупликация: GroupBy в памяти)
    ↓
RawTickRepository.BulkCopyAsync()
    ↓ (Binary COPY → temp table → INSERT ON CONFLICT DO NOTHING)
    ↓ (OTel: ticks.processed, batch size, write duration)
PostgreSQL (RawTicks)
```

**Параллельный поток агрегации свечей:**

```
ProcessTickAsync()
    ↓ (прямой вызов, fire-and-forget)
TickAggregator.OnTickAsync()
    ↓ (Channel<TickData> с DropOldest)
Фоновая задача ProcessChannelAsync
    ↓ (OHLCV агрегация в ConcurrentDictionary<AggregatorKey, InMemoryCandle>)
Таймер FlushCompletedCandlesAsync (каждые N секунд)
    ↓
[Kafka enabled] → KafkaCandleProducer → Topic aggregated-data → KafkaCandleConsumerService → PostgreSQL
[Kafka disabled] → IAggregatedDataRepository.AddRangeAsync() → PostgreSQL
```

### GC Optimization

В [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs) настроена оптимизация сборщика мусора для high-throughput сценария:

- `GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency` — минимизация пауз GC
- Периодическая LOH compaction (каждые 5 минут) — `GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce` + `GC.Collect(2, GCCollectionMode.Forced, blocking: false)`

### OpenTelemetry Configuration

Worker слушает HTTP на порту 5010 и предоставляет:

- `/metrics` — Prometheus scraping endpoint
- `/health` — Health check (PostgreSQL + Kafka)

Метрики также экспортируются через OTLP в Aspire Dashboard (`localhost:18889`).

### Принципы SOLID
1. **Single Responsibility** — каждый класс имеет одну ответственность: [`WebSocketConnectionManager`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs) — соединение, [`WebSocketMessageReceiver`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs) — приём, [`ExponentialReconnectStrategy`](src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs) — переподключение, [`SubscriptionManager`](src/MarketDataCollector.Core/Clients/SubscriptionManager.cs) — подписка
2. **Open/Closed** — новые биржи добавляются через наследование от [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs)
3. **Liskov Substitution** — клиенты бирж взаимозаменяемы через [`IExchangeWebSocketClient`](src/MarketDataCollector.Core/Interfaces/IExchangeWebSocketClient.cs)
4. **Interface Segregation** — интерфейсы разделены по функциональности (14 интерфейсов вместо одного монолитного)
5. **Dependency Inversion** — все зависимости через интерфейсы и DI-контейнер

### Паттерны проектирования
- **Repository** — для доступа к данным ([`IRawTickRepository`](src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs), [`IConnectionLogRepository`](src/MarketDataCollector.Core/Interfaces/IConnectionLogRepository.cs), [`IAggregatedDataRepository`](src/MarketDataCollector.Core/Interfaces/IAggregatedDataRepository.cs))
- **Factory** — для создания WebSocket клиентов ([`WebSocketClientFactory`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs)) с двухфазной инициализацией
- **Observer** — события WebSocket (`MessageReceived`, `Connected`, `Disconnected`, `ErrorOccurred`)
- **Strategy** — стратегия переподключения ([`IReconnectStrategy`](src/MarketDataCollector.Core/Interfaces/IReconnectStrategy.cs))
- **Channel** — асинхронная очередь с backpressure (Single Consumer: один канал с `SingleReader=true`; Multiple Consumers: N независимых каналов с per-ticker routing) + отдельный канал для Async Writer
- **Bridge** — разделение монолитного клиента на связанные, но независимые иерархии (ConnectionManager, MessageReceiver, SubscriptionManager, ReconnectStrategy)
- **Bulk Copy** — Binary COPY protocol (Npgsql) для массовой вставки (10-100x быстрее AddRangeAsync)
- **FIFO Cache (batch eviction)** — [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) с эвикцией 10% при переполнении (быстрее, чем по одному элементу)

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
| **Kafka (KRaft 3.9.0)** | Брокер сообщений для развязки компонентов | `localhost:9092` (внутренний) / `localhost:9094` (внешний) |
| **Kafdrop** | Веб-интерфейс для просмотра Kafka | `http://localhost:9000` |
| **Prometheus** | Сбор метрик с Worker (scrape `/metrics`) | `http://localhost:9091` |
| **Aspire Dashboard** | OpenTelemetry Dashboard (метрики, трейсинг, логи) | OTLP gRPC: `localhost:18889`, UI: `http://localhost:19000` |

Проверьте, что все контейнеры запущены:
```bash
docker ps
```

Ожидаемый вывод:
```
CONTAINER ID   IMAGE                         PORTS                                              NAMES
abc123         postgres:16-alpine            0.0.0.0:5433->5432/tcp                             marketdata-postgres
def456         apache/kafka:3.9.0            0.0.0.0:9092->9092/tcp,0.0.0.0:9094->9094/tcp      marketdata-kafka
ghi789         obsidiandynamics/kafdrop:latest 0.0.0.0:9000->9000/tcp                           marketdata-kafdrop
jkl012         prom/prometheus:v2.53.0       0.0.0.0:9091->9090/tcp                             marketdata-prometheus
mno345         mcr.microsoft.com/dotnet/aspire-dashboard:9.1  0.0.0.0:18889->18889/tcp, 0.0.0.0:19000->18888/tcp  marketdata-aspire-dashboard
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

**Доступ к Prometheus:**
- Откройте в браузере: http://localhost:9091
- Просматривайте метрики, стройте графики

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
  "Kestrel": {
    "Endpoints": {
      "Http": { "Url": "http://0.0.0.0:5010" }
    }
  },
  "OpenTelemetry": {
    "OtlpEndpoint": "http://localhost:18889",
    "ServiceName": "MarketDataCollector.Worker"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "ConnectionStrings": {
    "MarketDataDb": "Host=localhost;Port=5433;Database=MarketDataDb;Username=marketdata_user;Password=StrongPassword123!;sslmode=Disable;Keepalive=30;Include Error Detail=true;CommandTimeout=120"
  },
  "MarketDataProcessor": {
    "UseSingleConsumer": true,
    "ChannelCapacity": 150000,
    "MinBatchSize": 2500,
    "MaxBatchSize": 5000,
    "MinPartialBatchSize": 1000,
    "BatchChannelCapacity": 40,
    "FlushIntervalSeconds": 5,
    "DeduplicationCacheMaxSize": 10000,
    "BacklogLowThreshold": 3000,
    "BacklogHighThreshold": 10000,
    "WriteDurationWarningMs": 200.0,
    "ProcessBatchTraceSampling": 10
  },
  "TickAggregator": {
    "Enabled": false,
    "CandleIntervalSeconds": 60,
    "FlushIntervalSeconds": 5,
    "ChannelCapacity": 100000
  },
  "Kafka": {
    "Enabled": false,
    "BootstrapServers": "localhost:9094",
    "AggregatedDataGroupId": "marketdata-aggregated-group",
    "AggregatedDataTopic": "aggregated-data",
    "AcksTimeoutMs": 5000,
    "MessageMaxBytes": 1048576
  },
  "ExchangeOptions": {
    "Exchanges": [
      {
        "ExchangeName": "binance",
        "WebSocketUrl": "ws://localhost:5000/ws/{symbol}@trade"
      },
      {
        "ExchangeName": "binance2",
        "WebSocketUrl": "wss://stream.binance.com:9443/ws/{symbol}@trade"
      }
    ],
    "Readers": [
      { "ExchangeName": "binance", "Symbol": "btcusdt" },
      { "ExchangeName": "binance", "Symbol": "ethusdt" },
      { "ExchangeName": "binance", "Symbol": "solusdt" }
    ]
  },
  "WebSocketClient": {
    "ReconnectDelay": "00:00:05",
    "MaxReconnectDelay": "00:00:60",
    "MaxInternalReconnectAttempts": 3,
    "MaxSubscribeRetries": 3,
    "ReceiveBufferSize": 16384,
    "MaxMessageSize": 1048576,
    "DisposeTimeout": "00:00:05"
  }
}
```

> Примечание: файл `appsettings.json` содержит локальные настройки для разработки (например, `binance` → `ws://localhost:5000` для FakeTickServer). В продакшене используйте реальный URL биржи (`wss://stream.binance.com:9443/ws/{symbol}@trade`).

**Параметры конфигурации MarketDataProcessor:**
- `UseSingleConsumer` — `true` (по умолчанию): ровно 1 consumer + 1 writer. `false`: N параллельных consumer'ов с per-ticker routing
- `ConsumerCount` — количество consumer'ов для Multiple Consumers (0 = авто, Math.Clamp(CPU/2, 1, 4)). Используется только при `UseSingleConsumer=false`
- `ChannelCapacity` — ёмкость КАЖДОГО input-канала (150000). При N consumer'ов общая ёмкость = N × ChannelCapacity
- `MinBatchSize` — минимальный размер батча при адаптивном режиме (2500)
- `MaxBatchSize` — максимальный размер батча при адаптивном режиме (5000); если 0 — используется `BatchSize`
- `MinPartialBatchSize` — минимальный размер частичного батча при flush по таймеру (1000); защита от микробатчей
- `BatchChannelCapacity` — ёмкость канала между Collector и Writer (40 батчей); при переполнении — backpressure (FullMode=Wait)
- `FlushIntervalSeconds` — сброс неполных батчей по таймеру (5с)
- `DeduplicationCacheMaxSize` — размер in-memory кэша дедупликации (10000)
- `BacklogLowThreshold` — порог backlog для снижения батча (3000)
- `BacklogHighThreshold` — порог backlog для увеличения батча (10000)
- `WriteDurationWarningMs` — если запись батча заняла дольше этого времени (ms), BatchSize временно снижается на 20% (200.0)
- `ProcessBatchTraceSampling` — сэмплирование трейсов ProcessBatch: создавать Activity только для каждого N-го батча (10)

**Параметры конфигурации TickAggregator:**
- `Enabled` — включить агрегацию свечей (по умолчанию `false`)
- `CandleIntervalSeconds` — интервал свечи в секундах (1-3600, по умолчанию 60 = 1m)
- `FlushIntervalSeconds` — интервал сброса завершённых свечей (5с)
- `ChannelCapacity` — ёмкость Channel для буферизации тиков (100000)

**Параметры конфигурации Kafka:**
- `Enabled` — включить Kafka интеграцию (по умолчанию `false`)
- `BootstrapServers` — адрес Kafka брокера
- `AggregatedDataTopic` — топик для OHLCV-свечей
- `AggregatedDataGroupId` — consumer group ID для свечей

**Параметры OpenTelemetry:**
- `OtlpEndpoint` — endpoint OTLP gRPC (по умолчанию `http://localhost:18889` — Aspire Dashboard)
- `ServiceName` — имя сервиса для меток

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
      Starting 5 WebSocket clients with staggered startup (2s interval)...
info: MarketDataCollector.Worker[0]
      Health-check: 5 connected, 0 disconnected | fills: 0.0%, 0.0%, 0.0% | total: 0.0% | dropped: ~0 | RPS: Incoming=0.0 msg/s, Processed=0.0 ticks/s
```

Воркер работает непрерывно, автоматически переподключаясь при обрывах соединения.

## Добавление новой биржи

1. Создайте класс клиента, наследуемый от [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs):
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
| `AggregatedData` | Агрегированные данные по интервалам (OHLCV-свечи) |

### Ключевые индексы
- `RawTicks`: уникальный индекс `(Ticker, Exchange, Timestamp)` — защита от дубликатов
- `RawTicks`: индексы по `Ticker`, `Timestamp`, `Exchange` — быстрый поиск
- `ConnectionLogs`: индексы по `Exchange`, `CreatedAt`
- `AggregatedData`: индексы по `(Ticker, Interval)`, `StartTime`

### Партиционирование

Для таблицы `RawTicks` поддерживается партиционирование по дням (скрипт [`docker/init-partitioned.sql`](docker/init-partitioned.sql)). Варианты:
- **Вариант А:** Ручное управление партициями (создание на ±7 дней от текущей даты)
- **Вариант Б:** Автоматическое управление через расширение `pg_partman`

Схема создаётся автоматически через [`docker/init.sql`](docker/init.sql) и EF Core `OnModelCreating` в [`MarketDataDbContext`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs).

## Бенчмарки

### TickWriteBenchmark

Проект [`tests/TickWriteBenchmark/`](tests/TickWriteBenchmark/) сравнивает три метода записи тиков в PostgreSQL:

| Метод | Описание | Производительность |
|-------|----------|-------------------|
| **BinaryCopyDirect** | Прямой Binary COPY в таблицу | Самый быстрый, но без ON CONFLICT |
| **BulkCopyAsync** | Binary COPY → temp table → INSERT ON CONFLICT DO NOTHING | Production-путь (используется в системе) |
| **BulkInsertIgnoreConflicts** | Параметризованный INSERT с ON CONFLICT | Медленнее COPY, не требует temp table |

Также выполняет **READ-бенчмарк** — сравнение производительности SELECT-запросов на обычной таблице vs партиционированной (partition pruning).

**Запуск:**
```powershell
.\run_benchmark.ps1
```

Результаты сохраняются в [`read_baseline.json`](read_baseline.json) и [`read_partitioned.json`](read_partitioned.json).

### FakeTickServer (нагрузочное тестирование)

Проект [`tests/FakeTickServer/`](tests/FakeTickServer/) — симулятор биржи, генерирующий тики в формате Binance trade stream через WebSocket.

**Параметры:**
| Флаг | Описание | По умолчанию |
|------|----------|-------------|
| `--port` / `-p` | Порт WebSocket сервера | 5000 |
| `--rps` / `-r` | Количество тиков в секунду | 1000 |
| `--symbols` / `-s` | Список тикеров через запятую | btcusdt,ethusdt |
| `--base-price` / `-b` | Базовая цена в USD | 50000 |
| `--max-ticks` / `-m` | Максимум тиков (0 = без лимита) | 0 |
| `--dup-percent` / `-d` | Процент дубликатов (0-100) | 0 |

**Запуск:**
```powershell
.\run_fake_server.ps1
```

Скрипт [`run_fake_server.ps1`](run_fake_server.ps1) по умолчанию запускает генератор с нагрузкой **~25 000 тиков/сек** по 3 символам (`btcusdt,ethusdt,solusdt`), `--max-ticks 1700000` и `--dup-percent 3`. Параметры можно изменить в самом скрипте.

После запуска FakeTickServer нужно заменить в `appsettings.json` URL биржи на `ws://localhost:5000/ws/{symbol}@trade` и запустить Worker.

## Мониторинг и логирование

### Логи
- Уровни: Debug, Information, Warning, Error, Critical
- Выход: Console + OpenTelemetry (OTLP в Aspire Dashboard)
- События: подключение, отключение, ошибки, статистика, health-check
- Структурированные логи с IncludeFormattedMessage и IncludeScopes

### Метрики (через [`MonitoringService`](src/MarketDataCollector.Application/Services/MonitoringService.cs) + OpenTelemetry)
- Количество обработанных тиков по каждой бирже
- Статус подключений (Connected / Disconnected / Error)
- RPS входящих/обработанных (через [`SlidingWindowCounter`](src/MarketDataCollector.Core/Utilities/SlidingWindowCounter.cs))
- Channel fill %, backlog, дропы
- Fire-and-forget запись событий в `ConnectionLogs`

### Сбор метрик через Prometheus

Скрипт [`scripts/collect-counters.ps1`](scripts/collect-counters.ps1) периодически опрашивает `/metrics` endpoint и сохраняет метрики в CSV (директория по умолчанию `./traces`):

```powershell
.\scripts\collect-counters.ps1 -MetricsUrl "http://localhost:5010/metrics" -Duration 120
```

Параметры:
- `-MetricsUrl` — URL Prometheus metrics endpoint (по умолчанию `http://localhost:5010/metrics`)
- `-RefreshSeconds` — интервал опроса (5с)
- `-Duration` — максимальная длительность сбора в секундах (0 = без ограничений)
- `-OutputDir` — директория для CSV (по умолчанию `./traces`)

Метрики можно просматривать в реальном времени через Prometheus (http://localhost:9091) или Aspire Dashboard (http://localhost:19000). Для анализа собранных CSV используйте [`scripts/analyze_counters.ps1`](scripts/analyze_counters.ps1).

### Health-check (в [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs))
- Периодическая проверка каждые 10 секунд
- Автоматический перезапуск отключённых клиентов (через идемпотентный `StartAsync`)
- Логирование статуса: подключено/отключено, fill %, RPS, дропы
- Остановка Worker при критической ошибке `MarketDataProcessor`
- HTTP endpoint `/health` (проверка PostgreSQL + Kafka)

## Тестирование

### Модульные тесты (xUnit)

Проект [`tests/MarketDataCollector.Tests/`](tests/MarketDataCollector.Tests/) содержит 300+ модульных тестов:

| Категория | Файлы | Описание |
|-----------|-------|----------|
| **Application/Services** | `MarketDataProcessorTests.cs` (34 теста), `MonitoringServiceTests.cs`, `DeduplicationCacheTests.cs`, `TickAggregatorTests.cs` | Бизнес-логика |
| **Core/Clients** | `BaseWebSocketClientTests.cs`, `ExponentialReconnectStrategyTests.cs`, `SubscriptionManagerTests.cs`, `WebSocketConnectionManagerTests.cs`, `WebSocketMessageReceiverTests.cs` | Базовые компоненты |
| **Core/Configuration** | | Конфигурация |
| **DomainUtilities** | `DecimalHelperTests.cs` | Хелперы домена |
| **Infrastructure** | `BinanceWebSocketClientTests.cs`, `WebSocketClientFactoryTests.cs`, `RawTickRepositoryTests.cs`, `RawTickRepositoryDeadlockTests.cs` | Реализации |
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
- [`run_benchmark.ps1`](run_benchmark.ps1) — запуск TickWriteBenchmark
- [`run_fake_server.ps1`](run_fake_server.ps1) — запуск FakeTickServer (нагрузочное тестирование)
- [`metrics.ps1`](metrics.ps1) — диспетчер сбора метрик и профилирования (см. ниже)
- [`run_all_metrics.ps1`](run_all_metrics.ps1) — запуск полного профилирования (counters + trace + gcdump)

### Сбор метрик и профилирование

[`metrics.ps1`](metrics.ps1) — единый диспетчер, делегирующий вызов скриптам из папки `scripts/`:

| Режим (`-Mode`) | Что делает |
|-----------------|------------|
| `counters` | Сбор Prometheus-метрик в CSV (дефолт) |
| `trace` | `dotnet-trace` с allocation tracking (+ конвертация в SpeedScope) |
| `gcdump` | `dotnet-gcdump` (2 снапшота: на пике и после дренажа) |
| `all` | Всё сразу: counters + trace + 2× gcdump |

```powershell
.\metrics.ps1 -Mode counters
.\metrics.ps1 -Mode trace -TraceDuration 60
.\metrics.ps1 -Mode gcdump -GcDumpAtPeakSec 40
.\metrics.ps1 -Mode all -TraceDuration 90 -GcDumpAtPeakSec 50
```

Также можно запускать скрипты напрямую:

```powershell
.\scripts\collect-counters.ps1 -Duration 120
.\scripts\collect-trace.ps1 -TraceDuration 90
.\scripts\collect-gcdump.ps1 -GcDumpAtPeakSec 50
.\scripts\collect-all.ps1 -TraceDuration 90 -GcDumpAtPeakSec 50
```

Результаты сохраняются в [`traces/`](traces/) (по умолчанию). Для анализа CSV-файлов счётчиков используйте [`scripts/analyze_counters.ps1`](scripts/analyze_counters.ps1).

### Переменные окружения

Для продакшена рекомендуется использовать переменные окружения или User Secrets:

```bash
dotnet user-secrets init
dotnet user-secrets set "ConnectionStrings:MarketDataDb" "Host=...;Password=..."
```

### Конфигурация OpenTelemetry

Worker предоставляет HTTP endpoint на порту 5010:
- `/metrics` — Prometheus scraping
- `/health` — Health check (PostgreSQL + Kafka)

Для отключения OpenTelemetry в development среде можно оставить пустым OtlpEndpoint или использовать `appsettings.Development.json`.

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
3. Проверьте логику трёхуровневой дедупликации: [`DeduplicationCache`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs) → `GroupBy` → `ON CONFLICT DO NOTHING`

### Проблема: Воркер падает с ошибкой
**Решение:**
1. Проверьте логи на наличие `Critical` ошибок
2. Убедитесь, что процессор не завершился с ошибкой (`IsFaulted` task)
3. Проверьте `/health` endpoint для диагностики состояния PostgreSQL/Kafka
4. Перезапустите воркер — он восстановит соединения
5. Внешний оркестратор (Docker/K8s) автоматически перезапустит Worker

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
docker exec marketdata-kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic aggregated-data --partitions 3 --replication-factor 1
```
4. Перезапустите init-контейнер: `docker-compose up -d kafka-init-topics`

### Проблема: Не открывается Kafdrop (http://localhost:9000)
**Решение:**
1. Проверьте, что контейнер запущен: `docker ps | findstr kafdrop`
2. Проверьте логи: `docker logs marketdata-kafdrop`
3. Убедитесь, что порт 9000 не занят другим приложением
4. Kafdrop может стартовать с задержкой — обновите страницу через 10-15 секунд

### Проблема: Worker не видит метрики в Prometheus/Aspire Dashboard
**Решение:**
1. Проверьте, что Worker запущен: `docker ps | findstr marketdata-prometheus`
2. Проверьте конфигурацию OpenTelemetry в `appsettings.json`
3. Для Aspire Dashboard убедитесь, что порты 18889 (OTLP gRPC) и 19000 (UI) свободны
4. Для Prometheus проверьте конфигурацию scrape в [`docker/prometheus/prometheus.yml`](docker/prometheus/prometheus.yml)

## Лицензия

MIT License

## Контакты

Для вопросов и предложений создавайте issue в репозитории проекта.

telegram: @Omsk1984

---

*Последнее обновление: июль 2026 (актуализировано под Single Consumer и новые скрипты)*
