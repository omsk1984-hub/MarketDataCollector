# План: Интеграция Kafka в .NET код MarketDataCollector

## Цель

Добавить Apache Kafka как брокер сообщений для **агрегированных данных (OHLCV-свечей)**, чтобы развязать компонент агрегации от записи в PostgreSQL. Сырые тики (`raw-ticks`) по-прежнему пишутся напрямую в БД через [`MarketDataProcessor`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs).

Фокус — **только `aggregated-data`**.

---

## 1. Текущая архитектура (до изменений)

```
Binance WS → BinanceWebSocketClient → MarketDataProcessor
                                         │
                                         ├──→ Channel → Batch → PostgreSQL (raw-ticks)
                                         │
                                         └──→ TickAggregator → Channel → Timer
                                                                          │
                                                                          └──→ IAggregatedDataRepository → PostgreSQL
                                         
MonitoringService → ConnectionLog → PostgreSQL (fire-and-forget)
```

**Проблема:** Агрегатор свечей пишет напрямую в БД через репозиторий. Нет буферизации при недоступности БД. Нельзя добавить новых потребителей свечей.

---

## 2. Целевая архитектура (после интеграции)

```
Binance WS → BinanceWebSocketClient → MarketDataProcessor
                                         │
                                         ├──→ Channel → Batch → PostgreSQL (raw-ticks — БЕЗ ИЗМЕНЕНИЙ)
                                         │
                                         └──→ TickAggregator → Channel → Timer
                                                                          │
                                                                          └──→ KafkaCandleProducer (topic: aggregated-data)

                    ┌──────────────────────────────────┐
                    │         Kafka Broker              │
                    │  topic: aggregated-data (3p)      │
                    └──────────────┬───────────────────┘
                                   │
                           ┌───────▼────────┐
                           │ KafkaCandle     │
                           │ ConsumerService │
                           │ (BG service)    │
                           │ → IAggregatedDataRepository → PostgreSQL
                           └─────────────────┘
```

### Что меняется

| Компонент | Было | Стало |
|-----------|------|-------|
| [`MarketDataProcessor`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs) | Прямая запись raw-ticks в БД | **Без изменений** — запись как есть |
| [`TickAggregator`](src/MarketDataCollector.Application/Services/TickAggregator.cs) | Прямая запись свечей в БД | Публикация свечей в Kafka (topic: `aggregated-data`) |
| [`MonitoringService`](src/MarketDataCollector.Application/Services/MonitoringService.cs) | Fire-and-forget в БД | **Без изменений** (если не требуется) |
| **Новый: KafkaCandleConsumerService** | — | Читает свечи из Kafka, пишет в PostgreSQL |

### Преимущества

| Аспект | Было | Стало |
|--------|------|-------|
| Запись свечей | Синхронная в TickAggregator | Асинхронная через Kafka consumer |
| Отказоустойчивость | Потеря свечей при падении агрегатора | Kafka буферизирует |
| Масштабирование | Один процесс | Несколько consumer'ов в группе |
| Воспроизведение | Нет | Replay свечей из Kafka (retention 30 дней) |

---

## 3. Изменяемые/новые файлы

### 3.1. Инфраструктурный слой — [`src/MarketDataCollector.Infrastructure/`](src/MarketDataCollector.Infrastructure/)

#### Новые файлы:

| Файл | Назначение |
|------|------------|
| [`Kafka/IKafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/IKafkaProducer.cs) | Generic интерфейс Kafka producer'а |
| [`Kafka/KafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaProducer.cs) | Базовая реализация через `Confluent.Kafka` |
| [`Kafka/KafkaCandleProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs) | Producer для свечей (topic: `aggregated-data`) |
| [`Kafka/KafkaCandleConsumerService.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs) | Фоновый consumer — читает свечи, пишет в БД |

#### Структура директории Kafka:
```
src/MarketDataCollector.Infrastructure/
└── Kafka/
    ├── IKafkaProducer.cs                          (NEW)
    ├── KafkaProducer.cs                           (NEW)
    ├── KafkaCandleProducer.cs                     (NEW)
    └── KafkaCandleConsumerService.cs               (NEW)
```

### 3.2. Конфигурация — [`src/MarketDataCollector.Core/Configuration/`](src/MarketDataCollector.Core/Configuration/)

#### Новые файлы:

| Файл | Назначение |
|------|------------|
| [`KafkaOptions.cs`](src/MarketDataCollector.Core/Configuration/KafkaOptions.cs) | Настройки подключения к Kafka (только для aggregated-data) |

### 3.3. Application слой — [`src/MarketDataCollector.Application/Services/`](src/MarketDataCollector.Application/Services/)

#### Изменяемые файлы:

| Файл | Изменение |
|------|-----------|
| [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs) | Вместо прямой записи в БД через `IAggregatedDataRepository` — публикация в Kafka через `KafkaCandleProducer` |

**На заметку:** `MarketDataProcessor` и `MonitoringService` **не изменяются**.

### 3.4. Worker — [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/)

#### Изменяемые файлы:

| Файл | Изменение |
|------|-----------|
| [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs) | Регистрация `KafkaCandleProducer`, `KafkaCandleConsumerService`, `KafkaOptions` |
| [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) | Добавить секцию `Kafka` |

### 3.5. NuGet-пакет

| Проект | Пакет |
|--------|-------|
| [`MarketDataCollector.Infrastructure.csproj`](src/MarketDataCollector.Infrastructure/MarketDataCollector.Infrastructure.csproj) | `Confluent.Kafka` (latest stable) |

---

## 4. Детальное описание каждого компонента

### 4.1. [`KafkaOptions.cs`](src/MarketDataCollector.Core/Configuration/KafkaOptions.cs)

```csharp
namespace MarketDataCollector.Core.Configuration;

public class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>Адрес bootstrap-сервера Kafka.</summary>
    public string BootstrapServers { get; set; } = "localhost:9094";

    /// <summary>ID consumer-группы для aggregated-data.</summary>
    public string AggregatedDataGroupId { get; set; } = "marketdata-aggregated-group";

    /// <summary>Имя топика для агрегированных данных.</summary>
    public string AggregatedDataTopic { get; set; } = "aggregated-data";

    /// <summary>Таймаут подтверждения записи (acks).</summary>
    public int AcksTimeoutMs { get; set; } = 5000;

    /// <summary>Максимальный размер пакета (байт).</summary>
    public int MessageMaxBytes { get; set; } = 1048576; // 1MB

    /// <summary>Включить/отключить Kafka интеграцию.</summary>
    public bool Enabled { get; set; } = true;
}
```

### 4.2. [`IKafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/IKafkaProducer.cs)

```csharp
namespace MarketDataCollector.Infrastructure.Kafka;

/// <summary>
/// Generic Kafka producer для отправки сообщений.
/// </summary>
public interface IKafkaProducer<TKey, TValue>
{
    /// <summary>Отправить сообщение в топик.</summary>
    Task ProduceAsync(string topic, TKey key, TValue value, CancellationToken cancellationToken = default);

    /// <summary>Flush и освобождение ресурсов.</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}
```

### 4.3. [`KafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaProducer.cs)

Базовая реализация producer'а через `Confluent.Kafka`:
- Сериализация в JSON
- Настройка `acks=all` (гарантия записи на все реплики)
- Автоматический retry при временных ошибках
- Логирование ошибок и успешных отправок

### 4.4. [`KafkaCandleProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs)

Оборачивает `IKafkaProducer<string, string>`, формирует JSON из закрытой свечи (OHLCV):

```json
{
  "ticker": "btcusdt",
  "interval": "1m",
  "open": "45000.00",
  "high": "45100.00",
  "low": "44950.00",
  "close": "45080.00",
  "volume": "123.45",
  "startTime": "2026-05-17T12:34:00Z",
  "endTime": "2026-05-17T12:35:00Z",
  "exchange": "binance"
}
```

Ключ сообщения — `{ticker}:{exchange}` для гарантии порядка свечей по символу.

### 4.5. [`KafkaCandleConsumerService.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs)

Фоновый `BackgroundService`:
1. Подписывается на топик `aggregated-data`
2. Десериализует JSON в `AggregatedData`
3. Пишет в БД через `IAggregatedDataRepository`
4. Ручной commit offset'ов — **только после успешной записи** в БД

```csharp
public class KafkaCandleConsumerService : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaCandleConsumerService> _logger;
    private readonly KafkaOptions _options;
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_options.AggregatedDataTopic);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                await ProcessMessageAsync(result.Message, stoppingToken);
                _consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error");
            }
        }
    }
    
    private async Task ProcessMessageAsync(Message<string, string> message, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();
        
        var candle = JsonSerializer.Deserialize<AggregatedData>(message.Value);
        // Используем существующую логику AddRangeAsync + SaveChangesAsync
        // (или BulkInsert в будущем)
    }
}
```

---

## 5. Изменения в [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs)

### Текущее состояние — [`SaveCandlesAsync`](src/MarketDataCollector.Application/Services/TickAggregator.cs:240)

```csharp
private async Task SaveCandlesAsync(List<InMemoryCandle> candles)
{
    using var scope = _scopeFactory.CreateScope();
    var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();

    var entities = candles.Select(c => c.ToAggregatedData(_timeService)).ToList();
    await repository.AddRangeAsync(entities);
    await repository.SaveChangesAsync();
}
```

### Целевое состояние

```csharp
public class TickAggregator : ITickAggregator
{
    // ... существующие поля ...
    private readonly IKafkaProducer<string, string>? _kafkaCandleProducer;
    private readonly bool _useKafka;

    public TickAggregator(
        ITimeService timeService,
        ILogger<TickAggregator> logger,
        IServiceScopeFactory scopeFactory,
        IOptions<TickAggregatorOptions> options,
        IKafkaProducer<string, string>? kafkaCandleProducer = null,  // NEW
        IOptions<KafkaOptions>? kafkaOptions = null)                  // NEW
    {
        // ... существующая инициализация ...
        _kafkaCandleProducer = kafkaCandleProducer;
        _useKafka = kafkaOptions?.Value?.Enabled == true && kafkaCandleProducer != null;
    }
```

**Изменение `SaveCandlesAsync`:**

```csharp
private async Task SaveCandlesAsync(List<InMemoryCandle> candles)
{
    if (_useKafka && _kafkaCandleProducer != null)
    {
        // Публикуем каждую свечу в Kafka
        var topic = _kafkaOptions.AggregatedDataTopic;
        foreach (var candle in candles)
        {
            var key = $"{candle.Ticker}:{candle.Exchange}";
            var json = JsonSerializer.Serialize(new
            {
                ticker = candle.Ticker,
                interval = candle.Interval,
                open = candle.Open,
                high = candle.High,
                low = candle.Low,
                close = candle.Close,
                volume = candle.Volume,
                startTime = candle.StartTime.ToString("O"),
                endTime = candle.EndTime.ToString("O"),
                exchange = candle.Exchange
            });
            await _kafkaCandleProducer.ProduceAsync(topic, key, json, CancellationToken.None);
        }
        
        _logger.LogDebug("Опубликовано {Count} свечей в Kafka topic={Topic}", candles.Count, topic);
    }
    else
    {
        // Fallback: прямая запись в БД (как сейчас)
        using var scope = _scopeFactory.CreateScope();
        var repository = scope.ServiceProvider.GetRequiredService<IAggregatedDataRepository>();
        var entities = candles.Select(c => c.ToAggregatedData(_timeService)).ToList();
        await repository.AddRangeAsync(entities);
        await repository.SaveChangesAsync();
    }
}
```

### Что сохраняется без изменений:
- Вся логика агрегации свечей (`InMemoryCandle`, `RoundDown`, `FormatInterval`, `ProcessChannelAsync`)
- Таймер `FlushCompletedCandlesAsync`, финальный `FlushAllCandlesAsync`
- Интерфейс `ITickAggregator` — не меняется

---

## 6. Конфигурация

### [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) — новая секция

```json
{
  "Kafka": {
    "Enabled": true,
    "BootstrapServers": "localhost:9094",
    "AggregatedDataGroupId": "marketdata-aggregated-group",
    "AggregatedDataTopic": "aggregated-data",
    "AcksTimeoutMs": 5000,
    "MessageMaxBytes": 1048576
  }
}
```

---

## 7. Регистрация в DI

### [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs)

```csharp
// Kafka configuration
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
var kafkaSection = builder.Configuration.GetSection(KafkaOptions.SectionName);
var kafkaOptions = kafkaSection.Get<KafkaOptions>();

if (kafkaOptions?.Enabled == true)
{
    // Регистрируем producer как синглтон
    builder.Services.AddSingleton<IKafkaProducer<string, string>>(sp =>
    {
        var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
        var logger = sp.GetRequiredService<ILogger<KafkaProducer>>();
        return new KafkaProducer(options, logger);
    });
    
    builder.Services.AddSingleton<KafkaCandleProducer>();

    // Регистрируем consumer как hosted service
    builder.Services.AddHostedService<KafkaCandleConsumerService>();
}

// TickAggregator — singleton (уже зарегистрирован, обновляем конструктор)
// DI сам разрешит IKafkaProducer<string, string>? как null, если Kafka отключена
builder.Services.AddSingleton<ITickAggregator, TickAggregator>();
```

**Важно:** `IKafkaProducer<string, string>` регистрируется как `AddSingleton` — один экземпляр на всё приложение, с внутренним пулом соединений.

---

## 8. Обработка ошибок и гарантии доставки

### Producer side ([`TickAggregator -> KafkaCandleProducer`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs))

- Настройка `acks=all` — гарантия записи на все реплики
- Асинхронная отправка с внутренним буфером
- При недоступности Kafka — свечи накапливаются в `_activeCandles` (in-memory словарь)
- При переполнении Channel агрегатора — backpressure на `MarketDataProcessor`
- Логирование каждой ошибки отправки

### Consumer side ([`KafkaCandleConsumerService`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs))

- `enable.auto.commit = false` — ручной commit offset'ов
- Commit **только после успешной записи** в БД
- При ошибке записи — offset не коммитится, сообщение будет перечитано (at-least-once)
- Дедупликация при записи в БД — уже есть (ключ: ticker + exchange + startTime)

### Graceful degradation

- Если `Enabled: false` — `TickAggregator` пишет напрямую в БД (как сейчас)
- Если Kafka недоступна при старте — consumer логирует ошибку и ждёт
- Producer при недоступности Kafka: свечи остаются in-memory, flush по таймеру продолжает попытки

---

## 9. Тестирование

### Unit тесты:

| Тест | Что проверяет |
|------|---------------|
| KafkaCandleProducer.Serialization | Правильный JSON из AggregatedData |
| TicketAggregator.KafkaPublish | Публикация в Kafka, не в БД |
| TicketAggregator.Fallback | При `Enabled=false` запись в БД |
| KafkaCandleConsumer.ProcessMessage | Десериализация + запись в репозиторий |

### Интеграционные тесты (с Testcontainers):

```csharp
public class KafkaAggregatedDataIntegrationTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:latest")
        .Build();
    
    [Fact]
    public async Task CandleProduced_ShouldBeConsumedAndStored()
    {
        await _kafka.StartAsync();
        var bootstrapServers = _kafka.GetBootstrapAddress();
        
        // 1. Создаём топик
        // 2. Producer отправляет свечу
        // 3. Consumer читает и пишет в PostgreSQL (Testcontainer)
        // 4. Проверяем, что свеча сохранена
    }
}
```

---

## 10. Пошаговый план реализации

### Шаг 1: NuGet-пакет
- Добавить `Confluent.Kafka` в [`MarketDataCollector.Infrastructure.csproj`](src/MarketDataCollector.Infrastructure/MarketDataCollector.Infrastructure.csproj)

### Шаг 2: Конфигурация
- Создать [`KafkaOptions.cs`](src/MarketDataCollector.Core/Configuration/KafkaOptions.cs) — настройки подключения к Kafka

### Шаг 3: Интерфейс и базовая реализация producer'а
- Создать [`IKafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/IKafkaProducer.cs) — generic интерфейс
- Создать [`KafkaProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaProducer.cs) — реализация через `Confluent.Kafka`

### Шаг 4: KafkaCandleProducer
- Создать [`KafkaCandleProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs) — сериализует свечу в JSON, отправляет в `aggregated-data`

### Шаг 5: KafkaCandleConsumerService
- Создать [`KafkaCandleConsumerService.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs) — читает свечи из Kafka, пишет в БД

### Шаг 6: Изменение TickAggregator
- Обновить [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs) — добавить зависимость от `IKafkaProducer<string, string>`, публикация свечей в Kafka

### Шаг 7: DI-регистрация и конфигурация
- Обновить [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs) — регистрация producer'а, consumer'а, KafkaOptions
- Обновить [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) — секция `Kafka`

### Шаг 8: Тестирование
- Unit-тесты для producer'а, consumer'а, fallback-логики
- Интеграционные тесты с Testcontainers (опционально)

---

## 11. Sequence Diagram

```
TickAggregator           KafkaCandleProducer       Kafka           KafkaCandle           PostgreSQL
                                                                    ConsumerService
    │                              │                  │                  │                    │
    │ [Timer elapsed: flush]       │                  │                  │                    │
    │──SaveCandlesAsync()          │                  │                  │                    │
    │                              │                  │                  │                    │
    │──ProduceAsync(key, json)───>│                  │                  │                    │
    │                              │──Produce────────>│                  │                    │
    │                              │  (topic:         │                  │                    │
    │                              │   aggregated-    │                  │                    │
    │                              │   data)          │                  │                    │
    │                              │                  │                  │                    │
    │                              │                  │──Consume────────>│                    │
    │                              │                  │                  │──Deserialize       │
    │                              │                  │                  │──AddRangeAsync─────>│
    │                              │                  │                  │──SaveChangesAsync──>│
    │                              │                  │                  │──Commit ───────────│
    │                              │                  │                  │                    │
```

---

## 12. Риски и компромиссы

| Риск | Вероятность | Смягчение |
|------|------------|-----------|
| Потеря свечей при падении TickAggregator до отправки в Kafka | Низкая | Channel в памяти, но Kafka буферизирует последующие после восстановления |
| Дубликаты свечей при перебалансировке consumer'а | Средняя | Дедупликация при записи (уникальный ключ ticker+exchange+startTime) |
| Увеличение задержки от закрытия свечи до записи в БД | Средняя | Настраиваемый flush interval, моментальная отправка в Kafka |
| Kafka — дополнительный инфраструктурный компонент | Средняя | Флаг `Enabled: false` для отключения |

---

## 13. Итоговый список изменённых/новых файлов

```
НОВЫЕ (4 файла):
  src/MarketDataCollector.Core/Configuration/KafkaOptions.cs
  src/MarketDataCollector.Infrastructure/Kafka/IKafkaProducer.cs
  src/MarketDataCollector.Infrastructure/Kafka/KafkaProducer.cs
  src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs
  src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleConsumerService.cs

ИЗМЕНЁННЫЕ (3 файла):
  src/MarketDataCollector.Application/Services/TickAggregator.cs
  src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs
  src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json

БЕЗ ИЗМЕНЕНИЙ:
  src/MarketDataCollector.Application/Services/MarketDataProcessor.cs
  src/MarketDataCollector.Application/Services/MonitoringService.cs
```

**Итого:**
- **Новых файлов:** 5
- **Изменённых файлов:** 3
- **Не затрагиваются:** `MarketDataProcessor`, `MonitoringService`, `WebSocket`-клиенты, `Worker.cs`
