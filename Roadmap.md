# Future Roadmap: Улучшения и масштабирование

Документ описывает направления развития системы при увеличении нагрузки, добавлении оркестрации и внедрении production-практик.

---

## 1. Производительность и масштабирование базы данных

### 1.1. Bulk insert через Npgsql COPY

**Проблема:** [`MarketDataProcessor.ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:137) использует `AddRangeAsync` + `SaveChangesAsync` — это N отдельных INSERT-запросов в транзакции. При 1000+ тиков/сек это становится узким местом.

**Решение:** Заменить на `NpgsqlDataSource` с `COPY` (binary protocol):

```csharp
// Пример: Binary COPY для массовой вставки
await using var writer = dataSource.BeginBinaryImport(
    "COPY RawTicks (Id, Ticker, Price, Volume, Timestamp, Exchange, ReceivedAt, Normalized) " +
    "FROM STDIN (FORMAT BINARY)");

foreach (var tick in ticks)
{
    await writer.StartRowAsync(cancellationToken);
    await writer.WriteAsync(Guid.NewGuid(), NpgsqlTypes.NpgsqlDbType.Uuid);
    await writer.WriteAsync(tick.Ticker, NpgsqlTypes.NpgsqlDbType.Varchar);
    // ... остальные поля
}
await writer.CompleteAsync();
```

**Эффект:** ~10-50x ускорение записи по сравнению с `AddRangeAsync`.

### 1.2. Оптимизация дедупликации: один запрос вместо N

**Проблема:** [`GetExistingKeysFromDbAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:191) выполняет N отдельных `AnyAsync`-запросов (по одному на каждый тик в батче).

**Решение:** Заменить на один запрос с `WHERE IN`:

```csharp
// Вместо N запросов — один
var keys = ticks.Select(t => (t.Ticker, t.Exchange, t.Timestamp)).ToList();
var existing = await _dbSet
    .Where(t => keys.Contains(new { t.Ticker, t.Exchange, t.Timestamp }))
    .Select(t => new { t.Ticker, t.Exchange, t.Timestamp })
    .ToListAsync(cancellationToken);
```

**Эффект:** Снижение числа запросов с N до 1 на батч.

### 1.3. Пул соединений и управление подключениями

- Настроить `MaxPoolSize` в строке подключения (рекомендуется: `MaxPoolSize=100`)
- Добавить мониторинг пула через `NpgsqlDataSource` метрики
- Рассмотреть `PgBouncer` для production-среды (особенно при множестве воркеров)

### 1.4. Партиционирование таблиц

При объёме >100M записей в `RawTicks`:

```sql
-- Партиционирование по месяцам
CREATE TABLE RawTicks (
    Id UUID NOT NULL,
    Ticker VARCHAR(20) NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL,
    Exchange VARCHAR(50) NOT NULL
) PARTITION BY RANGE (Timestamp);

CREATE TABLE RawTicks_2026_01 PARTITION OF RawTicks
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

**Эффект:** Ускорение запросов с фильтром по времени, упрощение архивации старых данных.

### 1.5. Read-реплики PostgreSQL

- Направить запросы на чтение (аналитика, отчёты) на read-реплики
- Запись только в primary
- Использовать `NpgsqlDataSource` с балансировкой

---

## 2. Архитектурные улучшения

### 2.1. Выделение API-слоя

**Текущее состояние:** Нет REST/gRPC API — данные доступны только через прямые запросы к БД.

**Рекомендация:** Добавить отдельный Web API проект:

```
src/
├── MarketDataCollector.Api/              # REST API для доступа к данным
│   ├── Controllers/
│   │   ├── TicksController.cs            # GET /api/ticks?ticker=btcusdt&from=...
│   │   ├── ExchangesController.cs        # GET /api/exchanges/status
│   │   └── MetricsController.cs          # GET /api/metrics (Prometheus)
│   ├── Hubs/
│   │   └── TickHub.cs                    # SignalR для real-time подписки
│   └── Program.cs
```

**Эффект:** Возможность строить клиентские приложения, дашборды, интеграции.

### 2.2. Message Queue для развязки записи

**Проблема:** Прямая запись в БД из `MarketDataProcessor` создаёт связанность — при недоступности БД теряются данные.

**Решение:** Добавить промежуточный брокер сообщений:

```
Binance WS → Processor → Kafka/RabbitMQ/NATS → Consumer → PostgreSQL
                                              → Consumer → ClickHouse (аналитика)
                                              → Consumer → Redis (кэш последних тиков)
```

**Варианты:**
- **Kafka** — для высокой пропускной способности и долгосрочного хранения
- **RabbitMQ** — для надёжной доставки с подтверждениями
- **NATS JetStream** — лёгковесная альтернатива

**Эффект:**
- Разделение production и consumption
- Возможность множества consumer'ов с разной логикой
- Гарантия доставки (at-least-once)
- Репликация данных между ЦОД

### 2.3. Кэширование

Добавить многоуровневое кэширование:

| Уровень | Технология | Что кэшируем | TTL |
|---------|-----------|--------------|-----|
| L1 (in-memory) | `IMemoryCache` / `FusionCache` | Последние N тиков по символу | 1-5 сек |
| L2 (распределённый) | Redis | Агрегированные данные, статусы подключений | 10-60 сек |
| L3 (БД) | PostgreSQL | Исторические данные | — |

### 2.4. Graceful degradation

- **Circuit Breaker** для БД: при недоступности БД переключаться в режим "только сбор" (keepalive в памяти/файле)
- **Backpressure**: динамическая регулировка `ChannelCapacity` на основе скорости потребления
- **Fallback-хранилище**: при отказе PostgreSQL писать в локальный SQLite/файл, затем синхронизировать

---

## 3. Оркестрация и деплой

### 3.1. Docker Compose → Kubernetes

**Текущее состояние:** Один `docker-compose.yml` только для PostgreSQL.

**Целевая архитектура в K8s:**

```yaml
# Пример манифестов
apiVersion: apps/v1
kind: Deployment
metadata:
  name: marketdata-worker
spec:
  replicas: 3  # Горизонтальное масштабирование
  template:
    spec:
      containers:
      - name: worker
        image: marketdata-collector:latest
        env:
        - name: ConnectionStrings__MarketDataDb
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: connection-string
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          exec:
            command: ["dotnet", "MarketDataCollector.Worker.dll", "--health-check"]
          initialDelaySeconds: 30
          periodSeconds: 15
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: marketdata-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: marketdata-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### 3.2. Стратегия распределения символов

При множестве воркеров нужно распределять символы между ними, чтобы избежать дублирования подписок:

**Вариант A: Sharding по символу (статический)**
```yaml
# Каждый воркер получает свой набор символов через конфиг
# Worker-1: btcusdt, ethusdt
# Worker-2: solusdt, xrpusdt
# Worker-3: adausdt, dogeusdt
```

**Вариант B: Динамическое распределение через координатор**
```
Worker-1 ─┐
Worker-2 ─┤── Redis/K8s Lease ── Символы распределяются динамически
Worker-3 ─┘
```

**Вариант C: Kafka Consumer Group (если внедрён Kafka)**
- Каждый символ → отдельный Kafka partition
- Consumer'ы в группе автоматически распределяют партиции

### 3.3. Health-check endpoint для K8s

Добавить HTTP-endpoint для liveness/readiness probes:

```csharp
// В Program.cs добавить Minimal API endpoint
app.MapGet("/health", async (IMonitoringService monitoring) =>
{
    var status = monitoring.GetAllStatuses();
    var unhealthy = status.Values.Any(s => s == ConnectionStatus.Error);
    
    return unhealthy 
        ? Results.StatusCode(503)  // Readiness probe: не готов
        : Results.Ok(new { 
            status = "healthy",
            connected = status.Count(s => s.Value == ConnectionStatus.Connected),
            total = status.Count
        });
});
```

### 3.4. ConfigMap и динамическая перезагрузка

Вынести конфигурацию в K8s ConfigMap с поддержкой hot-reload:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: marketdata-config
data:
  appsettings.json: |
    {
      "ExchangeOptions": {
        "Exchanges": [...],
        "Readers": [...]
      },
      "MarketDataProcessor": {
        "BatchSize": 100,
        "ChannelCapacity": 10000
      }
    }
```

Использовать `IOptionsSnapshot<T>` для автоматической перезагрузки при изменении ConfigMap.

---

## 4. Мониторинг и observability

### 4.1. Метрики (Prometheus + Grafana)

Добавить экспорт метрик через `OpenTelemetry` или `prometheus-net`:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `ticks_received_total` | Counter | Всего получено тиков (по бирже, символу) |
| `ticks_processed_total` | Counter | Сохранено в БД |
| `ticks_duplicates_total` | Counter | Отброшено дубликатов |
| `websocket_connections` | Gauge | Текущее количество подключений |
| `websocket_reconnects_total` | Counter | Количество переподключений |
| `channel_queue_size` | Gauge | Текущий размер очереди Channel |
| `batch_process_duration` | Histogram | Время обработки батча |
| `db_write_duration` | Histogram | Время записи в БД |

### 4.2. Распределённый трейсинг (OpenTelemetry)

```csharp
// В Program.cs
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddNpgsql()
        .AddSource("MarketDataCollector")
        .AddOtlpExporter(options => 
            options.Endpoint = new Uri("http://otel-collector:4317")))
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddPrometheusExporter());
```

**Эффект:** End-to-end трассировка: WebSocket → Processor → DB, выявление узких мест.

### 4.3. Структурированное логирование (Serilog)

Заменить `ILogger` на Serilog с выгрузкой в Elasticsearch/Loki:

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(new JsonFormatter())
    .WriteTo.Seq("http://seq:5341")
    .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(new Uri("http://elastic:9200")))
    .Enrich.WithProperty("Application", "MarketDataCollector")
    .Enrich.WithEnvironmentName()
    .CreateLogger();
```

### 4.4. Дашборд реального времени

Добавить SignalR Hub + простой веб-дашборд:

```
Worker → SignalR Hub → Browser Dashboard
                        ├── Статус подключений (зелёный/жёлтый/красный)
                        ├── Тики/сек по каждому символу
                        ├── Задержка обработки (latency)
                        └── Ошибки и переподключения
```

---

## 5. Безопасность

### 5.1. Управление секретами

- **Локально:** `dotnet user-secrets` (уже описано в README)
- **Docker:** Docker Secrets или `.env` файлы
- **K8s:** External Secrets Operator + HashiCorp Vault / AWS Secrets Manager

### 5.2. Rate limiting для WebSocket

Добавить защиту от превышения лимитов биржи:

```csharp
public class RateLimitingWebSocketClient : BaseWebSocketClient
{
    private readonly RateLimiter _rateLimiter = 
        new SlidingWindowRateLimiter(new SlidingWindowRateLimiterOptions
        {
            PermitLimit = 10,       // 10 запросов
            Window = TimeSpan.FromSeconds(1),  // в секунду
            SegmentsPerWindow = 10
        });
    
    protected override async Task SubscribeToTickerAsync(string symbol, CancellationToken ct)
    {
        using var lease = await _rateLimiter.AcquireAsync(ct);
        if (!lease.IsAcquired)
            throw new RateLimitExceededException();
        
        await base.SubscribeToTickerAsync(symbol, ct);
    }
}
```

### 5.3. Валидация входных данных

- Добавить валидацию JSON-сообщений от бирж (размер, структура, типы данных)
- Защита от инъекций через `Ticker`/`Exchange` (хотя EF Core параметризует запросы)

---

## 6. Обработка данных и аналитика

### 6.1. Агрегация свечей (OHLCV)

Реализовать in-memory агрегацию перед записью в `AggregatedData`:

```csharp
public class CandleAggregator
{
    private readonly Dictionary<string, Candle> _activeCandles = new();
    
    public void OnTick(TickData tick)
    {
        var key = $"{tick.Ticker}_{tick.Exchange}";
        var bucket = RoundToInterval(tick.Timestamp, TimeSpan.FromMinutes(1));
        
        if (!_activeCandles.TryGetValue(key, out var candle) || candle.Bucket != bucket)
        {
            // Сбросить старую свечу в БД
            SaveCandle(candle);
            // Начать новую
            candle = new Candle { Bucket = bucket, Open = tick.Price };
        }
        
        candle.High = Math.Max(candle.High, tick.Price);
        candle.Low = Math.Min(candle.Low, tick.Price);
        candle.Close = tick.Price;
        candle.Volume += tick.Volume;
    }
}
```

### 6.2. Экспорт в аналитические БД

- **ClickHouse** — для аналитики по тиковым данным (колоночное хранение, ~10x сжатие)
- **TimescaleDB** — если оставаться в PostgreSQL-экосистеме (гипертаблицы, непрерывная агрегация)

### 6.3. Очистка старых данных (Retention Policy)

```sql
-- Автоматическое удаление данных старше N дней через pg_cron
SELECT cron.schedule('cleanup-raw-ticks', '0 3 * * *', 
    $$DELETE FROM RawTicks WHERE Timestamp < NOW() - INTERVAL '90 days'$$);
```

Или через партиционирование: просто удалять старые партиции.

---

## 7. Тестирование и качество

### 7.1. Интеграционные тесты с Testcontainers

```csharp
public class MarketDataProcessorIntegrationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = 
        new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithDatabase("MarketDataDbTest")
            .Build();
    
    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
        // Накатить миграции
        // Создать реальный MarketDataProcessor с подключением к контейнеру
    }
    
    [Fact]
    public async Task ProcessTickAsync_ShouldPersistToDatabase()
    {
        // Тест с реальной PostgreSQL в Docker-контейнере
    }
    
    public async Task DisposeAsync() => await _postgres.DisposeAsync();
}
```

### 7.2. Load-тестирование

Сценарии для `k6` или `NBomber`:

- Симуляция 1000+ тиков/сек от нескольких символов
- Проверка стабильности при переподключениях
- Тестирование поведения при недоступности БД

### 7.3. Chaos Engineering

- Внезапное отключение PostgreSQL
- Задержки сети (через `toxiproxy`)
- Удвоение нагрузки (traffic spike)
- "Thundering herd" при одновременном переподключении всех клиентов

---

## 8. Приоритеты внедрения

| Приоритет | Улучшение | Ожидаемый эффект | Сложность |
|-----------|-----------|-----------------|-----------|
| 🔴 P0 | Bulk insert через COPY | 10-50x ускорение записи | Средняя |
| 🔴 P0 | Один запрос дедупликации вместо N | Снижение нагрузки на БД | Низкая |
| 🔴 P0 | Health-check endpoint | K8s readiness/liveness | Низкая |
| 🟡 P1 | OpenTelemetry метрики + Prometheus | Видимость системы | Средняя |
| 🟡 P1 | Serilog + централизованное логирование | Быстрое выявление проблем | Средняя |
| 🟡 P1 | Message Queue (Kafka/NATS) | Развязка компонентов | Высокая |
| 🟡 P1 | K8s манифесты + HPA | Горизонтальное масштабирование | Средняя |
| 🟢 P2 | Партиционирование таблиц | Производительность при >100M записей | Средняя |
| 🟢 P2 | Read-реплики PostgreSQL | Масштабирование чтения | Средняя |
| 🟢 P2 | API-слой + SignalR дашборд | Доступность данных | Средняя |
| 🟢 P2 | Кэширование (Redis) | Снижение нагрузки на БД | Средняя |
| 🔵 P3 | ClickHouse для аналитики | Аналитические запросы | Высокая |
| 🔵 P3 | Chaos Engineering тесты | Надёжность системы | Высокая |
| 🔵 P3 | Rate limiting | Защита от лимитов бирж | Низкая |

---

## 9. Целевая архитектура (через 12 месяцев)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                          │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Worker Pod   │  │ Worker Pod   │  │ Worker Pod   │  ... (HPA)   │
│  │ (symbols:    │  │ (symbols:    │  │ (symbols:    │              │
│  │  btc, eth)   │  │  sol, xrp)   │  │  ada, doge)  │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                       │
│         └─────────────────┼─────────────────┘                       │
│                           │                                         │
│                    ┌──────▼───────┐                                 │
│                    │   Kafka      │  (или NATS JetStream)           │
│                    │  (topics:    │                                 │
│                    │   raw-ticks, │                                 │
│                    │   candles,   │                                 │
│                    │   events)    │                                 │
│                    └──────┬───────┘                                 │
│                           │                                         │
│          ┌────────────────┼────────────────┐                        │
│          │                │                │                        │
│   ┌──────▼───────┐ ┌──────▼───────┐ ┌──────▼───────┐               │
│   │ DB Consumer  │ │ Analytics    │ │ Cache        │               │
│   │ (PostgreSQL) │ │ Consumer     │ │ Consumer     │               │
│   │              │ │ (ClickHouse) │ │ (Redis)      │               │
│   └──────────────┘ └──────────────┘ └──────────────┘               │
│                                                                     │
│   ┌──────────────────────────────────────────────────────────┐      │
│   │  MarketDataCollector.Api (Deployment, HPA)               │      │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐  │      │
│   │  │ REST API │ │SignalR   │ │ Prometheus│ │ gRPC       │  │      │
│   │  │          │ │ Hub      │ │ Metrics  │ │ Stream     │  │      │
│   │  └──────────┘ └──────────┘ └──────────┘ └────────────┘  │      │
│   └──────────────────────────────────────────────────────────┘      │
│                                                                     │
│   ┌──────────────────────────────────────────────────────────┐      │
│   │  Observability Stack                                       │      │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐  │      │
│   │  │Grafana   │ │Prometheus│ │  Loki    │ │ Tempo      │  │      │
│   │  │(dashboards)│ │(metrics) │ │(logs)    │ │(tracing)   │  │      │
│   │  └──────────┘ └──────────┘ └──────────┘ └────────────┘  │      │
│   └──────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. Заключение

Текущая архитектура проекта закладывает хорошую основу: чистая слоистая структура, SOLID-принципы, делегирование ответственности. Основные узкие места для production-нагрузки:

1. **Запись в БД** — N запросов на батч (дедупликация + INSERT)
2. **Монолитность** — всё в одном процессе, нет горизонтального масштабирования
3. **Observability** — только консольные логи, нет метрик и трейсинга
4. **Отказоустойчивость** — нет буферизации при недоступности БД

Рекомендуется начать с P0-задач (bulk insert, оптимизация дедупликации, health-check endpoint) — они дают максимальный эффект при минимальных затратах.