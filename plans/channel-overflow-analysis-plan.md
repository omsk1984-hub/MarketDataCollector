# Анализ переполнения канала (Channel fill > 100%)

## Проблема

В логах health-check наблюдается `Channel fill: 128.8% (90157/70000)` и вплоть до `187.9% (131528/70000)`. Это указывает на две проблемы:

1. **Ошибка расчёта** — метрика `Channel fill %` считает fill как `channelCount / channelCapacity`, но `channelCount` (`GetChannelCount()`) возвращает сумму РАЗМЕРОВ ВСЕХ каналов (3 × 70000), а `channelCapacity` (`GetChannelCapacity()`) — ёмкость ОДНОГО канала (70000). Реальная fill: 90157 / (70000 × 3) = **42.9%**.

2. **Реальное переполнение** несмотря на это — `backlog` (incoming - received) достигает 266 584 тиков при суммарной ёмкости 210 000. Из-за `BoundedChannelFullMode.DropOldest` тики **тихо дропаются** без возможности обнаружения через `TryWrite`.

---

## Root Cause Analysis

### 1. Silent Drop через DropOldest

`BoundedChannelFullMode.DropOldest` при переполнении вытесняет самый старый элемент и успешно записывает новый — `TryWrite` возвращает `true` [MarketDataProcessor.cs:137](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:137). Поэтому `_totalDroppedCount` **всегда 0**, а предупреждение в [Worker.cs:223-230](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:223) никогда не срабатывает.

**Масштаб потерь:** ~199 094 тиков (16.6% от 1 200 000) были тихо дропнуты.

### 2. Consumer не успевает за Producer

Скорость входящих сообщений: ~19 000 msg/s стабильно [Worker.cs:176-178](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:176).

Скорость обработки колеблется:
- Пик: ~24 484 ticks/s
- Спад: ~7 187 ticks/s (GC/contention spikes)
- Средняя: ~16 000-19 000 ticks/s

**Причины падения скорости:**

#### a) GC Gen2 паузы (из counters-analysis)
| Метрика | Значение |
|---|---|
| Gen2 GC count за 15 сек | 4 |
| Allocation rate | ~150 MB/сек |
| GC pause total (ns) | 177 ms за 15 сек |
| LOH fragmentation | 13% (2.8 MB) |

Каждый Gen2 GC останавливает все managed потоки (STW). При 4 Gen2 GC за 15 сек, потери ≈ 4 × ~100-200 мс = **400-800 мс простоя на каждые 15 сек**.

#### b) Выбросы длительности записи в БД
- Средняя: ~105 мс на batch=2000
- Один batch на **3.96 сек** (вероятно совпал с Gen2 GC)
- При падении скорости до 7K ticks/s время записи растёт, backlog растёт, channel переполняется

#### c) Lock contention
- 1 220 lock contention'ов за 15 сек
- Monitor lock contention растёт с 83 до 1 220 за 15 сек

#### d) Аллокации в hot path

В [ProcessTickAsync:107-155](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:107):
- Создание `TickData` на каждом tick (~19K раз/сек) — аллокация managed объекта

В [ProcessBatchAsync:586-588](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:586):
- `filteredTicks.Select(t => new RawTick(...)).ToList()` — аллокация новых сущностей

В [BulkCopyAsync:253-263](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:253):
- `prices[i] = e.Price.ToString(CultureInfo.InvariantCulture)` — аллокация строк
- `volumes[i] = e.Volume.ToString(CultureInfo.InvariantCulture)` — аллокация строк
- Каждый `ToString()` создаёт новую строку ~19 000 * 2 = 38 000 аллокаций/сек только на `Price` и `Volume`

### 3. Структурная проблема: все потребители дропают

С конфигурацией `ConsumerCount=3` и `ChannelCapacity=70000` на каждый из 3-х каналов, суммарная ёмкость буфера — 210 000. Но:
- Ticks распределяются по каналам через `tickerHash` [MarketDataProcessor.cs:134](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:134)
- Все 3 тикера (btc, eth, sol) распределяются равномерно
- Каждый consumer обрабатывает ~⅓ всех тиков
- Если один consumer тормозит (GC, DB spike), его канал переполняется, дропаются тики **только его тикеров**

---

## Формат вывода health-check (новый)

**Текущий (избыточный):**
```
Health-check: 3 connected, 0 disconnected | RPS: Incoming=19011.4 msg/s, Processed=24484.0 ticks/s | Totals: WS_msgs=958631, Channel_in=958631, Channel_received=706000 | Channel fill: 128.8% (90157/70000) | Clients: binance_btcusdt=6410,3, binance_ethusdt=6268,4, binance_solusdt=6332,7
Health-check: Backlog канала: 252631 тиков (incoming=958631, received=706000). Тики в очереди/батче, не дропнуты.
```

**Новый (лаконичный):**
```
Health-check: 3 connected, 0 disconnected | fills: 42.9%, 38.1%, 47.7% | total: 42.9% | dropped: ~12583 | RPS: Incoming=19011.4 msg/s, Processed=24484.0 ticks/s
```

Changes:
- Проценты по **каждому каналу через запятую** в начале строки
- `fills: 42.9%, 38.1%, 47.7%` — сразу видно какой канал перегружен
- `total: 42.9%` — общий fill (correct: 90157/210000)
- `dropped: ~12583` — оценка реально дропнутых (раньше было невидимо)
- **Убрано**: `Totals: WS_msgs`, `Channel_in`, `Channel_received`, `Clients details`, `Backlog info`

---

## План исправлений (6 задач)

### 1. Добавить методы в IMarketDataProcessor

В [IMarketDataProcessor.cs](src/MarketDataCollector.Core/Interfaces/IMarketDataProcessor.cs) добавить:

```csharp
/// <summary>
/// Заполненность каждого канала: (Count, Capacity) по каждому consumer'у.
/// </summary>
(int Count, int Capacity)[] GetChannelFillLevels();

/// <summary>
/// Оценка реально дропнутых тиков через DropOldest.
/// max(0, incoming - received - сумма Reader.Count по всем каналам).
/// </summary>
int GetEstimatedDroppedCount();
```

### 2. Реализовать методы в MarketDataProcessor.cs

В [MarketDataProcessor.cs:686-694](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:686):

```csharp
public (int Count, int Capacity)[] GetChannelFillLevels()
{
    var result = new (int Count, int Capacity)[_channels.Length];
    for (int i = 0; i < _channels.Length; i++)
    {
        result[i] = (_channels[i].Reader.Count, _channelCapacity);
    }
    return result;
}

public int GetEstimatedDroppedCount()
{
    int droppedByChannel = _totalIncomingCount - _totalReceivedCount - GetChannelCount();
    return Math.Max(0, droppedByChannel);
}
```

### 3. Переписать health-check лог в Worker.cs

Новый формат (вместо текущих строк 186-240):

```csharp
// Per-channel fill percentages
var fillLevels = marketDataProcessor.GetChannelFillLevels();
var fillPercents = string.Join(", ", fillLevels.Select(f =>
    f.Capacity > 0 ? $"{(double)f.Count / f.Capacity * 100.0:F1}%" : "0%"));

// Total fill
var totalCount = fillLevels.Sum(f => f.Count);
var totalCapacity = fillLevels.Sum(f => f.Capacity);
var totalFillPercent = totalCapacity > 0
    ? (double)totalCount / totalCapacity * 100.0
    : 0.0;

// Estimated dropped
var estimatedDropped = marketDataProcessor.GetEstimatedDroppedCount();

_logger.LogInformation(
    "Health-check: {Connected} connected, {Disconnected} disconnected | " +
    "fills: {FillPercents} | total: {TotalFill:F1}% | dropped: ~{Dropped} | " +
    "RPS: Incoming={IncomingRps:F1} msg/s, Processed={ProcessedRps:F1} ticks/s",
    connected, disconnected, fillPercents, totalFillPercent, estimatedDropped,
    incommingRps, processedRps);
```

**Убрать полностью:**
- `Totals: WS_msgs`, `Channel_in`, `Channel_received`, `Channel fill`, `Clients details`
- Отдельный `Health-check: Backlog канала...` info-лог
- Предупреждение о расхождении счётчиков `wsVsChannelDiff` (никогда не срабатывало)

**Оставить:**
- Warning при `estimatedDropped > 100`

### 4. Добавить OpenTelemetry метрики

В [MarketDataTelemetry.cs](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs):

```csharp
public static readonly UpDownCounter<long> ChannelFillLevel = Instance.CreateUpDownCounter<long>(
    name: "processor.channel.fill_level",
    unit: "count",
    description: "Current ticks in channel by channel_index");

public static readonly Counter<long> TicksDroppedSilently = Instance.CreateCounter<long>(
    name: "ticks.dropped.silently",
    unit: "count",
    description: "Estimated ticks dropped silently by DropOldest mode");

public static readonly UpDownCounter<long> ChannelBacklog = Instance.CreateUpDownCounter<long>(
    name: "processor.channel.backlog",
    unit: "count",
    description: "Channel backlog (incoming - received)");
```

Обновлять метрики в health-check loop.

### 5. Увеличить ChannelCapacity

`appsettings.json:24`: `ChannelCapacity`: `70000` → `150000`

### 6. Оптимизировать аллокации

- `BulkCopyAsync`: `ArrayPool<string>` для price/volume строк, или `decimal[]` напрямую
- `ProcessBatchAsync`: убрать двойной проход `Select().ToList()` → `BulkCopyAsync`

---

## План работ

```mermaid
flowchart TD
    A[GetChannelFillLevels + GetEstimatedDroppedCount] --> B[IMarketDataProcessor.cs]
    A --> C[MarketDataProcessor.cs]
    B --> D[Переписать health-check Worker.cs]
    C --> D
    D --> E[Новый формат: fills: % , % , % | total | dropped]
    D --> F[Убрать избыточные логи]
    E --> G[OpenTelemetry метрики]
    G --> H[MarketDataTelemetry.cs]
    G --> I[Worker.cs health-check loop]
    D --> J[Увеличить ChannelCapacity]
    J --> K[appsettings.json: 70000 → 150000]
    D --> L[Оптимизация аллокаций]
    L --> M[RawTickRepository.cs BulkCopyAsync]
```

## Очерёдность выполнения

| № | Задача | Файлы | Приоритет |
|---|--------|-------|-----------|
| 1 | Добавить методы в IMarketDataProcessor + MarketDataProcessor | `IMarketDataProcessor.cs`, `MarketDataProcessor.cs` | High |
| 2 | Переписать health-check (новый формат) | `Worker.cs` | High |
| 3 | Добавить OpenTelemetry метрики | `MarketDataTelemetry.cs`, `Worker.cs` | Medium |
| 4 | Увеличить ChannelCapacity | `appsettings.json` | Medium |
| 5 | Оптимизация аллокаций | `RawTickRepository.cs`, `MarketDataProcessor.cs` | Low |
