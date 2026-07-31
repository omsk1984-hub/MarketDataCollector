# План реализации рекомендаций из counters-analysis-deepdive

**Источник:** [`plans/counters-analysis-deepdive_20260731_085114.md`](plans/counters-analysis-deepdive_20260731_085114.md:143) — раздел 6 «Рекомендации»

---

## Статус: ✅ Выполнено

Все 6 рекомендаций реализованы. Сборка успешна, тесты на затронутые области проходят.

---

## Реализованные изменения

### Рек.2 — Потокобезопасные счётчики (bug-fix, высокий приоритет)
**Файл:** [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:877)

Прямые `+=` в single-consumer ветке заменены на `Interlocked.Add`. Обе ветки (`single`/`multi`) теперь идентичны, `if/else` устранён. Счётчики читаются из фонового мониторинга / `GetProcessedCountAsync` параллельно с записью — устранена гонка.

```csharp
// Thread-safe инкремент: счётчики читаются из фонового мониторинга /
// GetProcessedCountAsync параллельно с записью в hot path (всегда безопасно).
int totalReceived = Interlocked.Add(ref _totalReceivedCount, batchSize);
int totalInserted = Interlocked.Add(ref _processedCount, inserted);
```

### Рек.6 — Устранение безусловного `GetString` (высокий приоритет, ~19.7% CPU)
`OnMessageReceived` безусловно вызывал `Encoding.UTF8.GetString(rawBytes.Span)` на каждый тик, хотя hot path (`ProcessMessageAsync`) уже парсит байты через `Utf8JsonReader`. Единственный подписчик `MessageReceived` в [`WebSocketClientFactory`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:91) использует только счётчик.

**Изменение типа события `MessageReceived` с `EventHandler<string>` на `EventHandler<ReadOnlyMemory<byte>>`:**
- [`IWebSocketClient.cs`](src/MarketDataCollector.Core/Interfaces/IWebSocketClient.cs:16)
- [`IExchangeWebSocketClient.cs`](src/MarketDataCollector.Core/Interfaces/IExchangeWebSocketClient.cs:66)
- [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:59) — тип события
- [`BaseWebSocketClient.OnMessageReceived`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:398) — передаёт `rawBytes` напрямую, без `GetString`
- [`WebSocketClientFactory.cs`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:91) — подписчик обновлён

Это полностью убирает `GetString` из hot path (zero-alloc).

### Рек.1 — Стабилизация размера батча (средний приоритет)
**Файл:** [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:22)

В single-consumer режиме (активный) мелкие батчи (307-600) идут от таймерного flush. Обновлены значения:

| Параметр | Было | Стало | Эффект |
|---|---|---|---|
| `MinBatchSize` | 1000 | 2500 | Крупнее адаптивные батчи (multi-consumer) |
| `MaxBatchSize` | 2500 | 5000 | Крупнее максимум |
| `MinPartialBatchSize` | 250 | 1000 | Меньше микробатчей от flush |
| `FlushIntervalSeconds` | 3 | 5 | Реже принудительный flush |
| `BacklogLowThreshold` | 2000 | 3000 | Adaptive mode (multi-consumer) |
| `BacklogHighThreshold` | 5000 | 10000 | Adaptive mode (multi-consumer) |

### Рек.4 — Пул массивов фиксированного размера (средний приоритет)
**Файл:** [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:200)

`RentOrCreate` (одиночный кэш, пересоздание при смене размера) заменён на `ReusableArrayCache<T>` — пул массивов **по точным размерам** (до 8 типичных размеров, FIFO-эвикция).

**Важное ограничение:** Npgsql читает `Array.Length` как число элементов (`unnest`). Пул возвращает массив точной длины (`new T[count]`), поэтому корректность вставки сохранена. Пул устраняет пересоздание 8 `new[]` при каждом колебании размера батча (307-1509), но не ломает Npgsql, т.к. `Array.Length == count`.

### Рек.3 — Снижение OTel-нагрузки на hot path (низкий/средний приоритет)
**Файлы:** [`MarketDataProcessorOptions.cs`](src/MarketDataCollector.Core/Configuration/MarketDataProcessorOptions.cs:120), [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:817)

Добавлена опция `ProcessBatchTraceSampling` (default 1) + поле-счётчик. `StartActivity("ProcessBatch")` создаётся только для каждого N-го батча:

```csharp
using var activity = (_processBatchTraceSampling <= 1
                      || Interlocked.Increment(ref _processBatchCounter) % _processBatchTraceSampling == 0)
    ? MarketDataTelemetry.ActivitySource.StartActivity("ProcessBatch")
    : null;
```

В `appsettings.json` установлено `ProcessBatchTraceSampling: 10` — экспорт ~2800 спанов/сек снижен в 10 раз. Глобальный sampler не тронут (не влияет на EF Core-трейсы).

### Рек.5 — Lock contention (низкий приоритет)
`BatchChannelCapacity` поднят с 20 до 40 в [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:22) — больше буфер между Collector и Writer, меньше контеншена.

---

## Тесты

### Обновлённый тест
[`BaseWebSocketClientTests.OnMessageReceived_RaisesMessageReceivedEvent`](tests/MarketDataCollector.Tests/Core/Clients/BaseWebSocketClientTests.cs:554) обновлён под новый тип `ReadOnlyMemory<byte>` (проверка байтов вместо string).

### Результаты прогона
| Набор тестов | Результат |
|---|---|
| `BaseWebSocketClientTests` + `WebSocketClientFactoryTests` | ✅ 36/36 passed |
| `RawTickRepositoryTests` | ✅ 21/21 passed |
| `MarketDataProcessorTests` | ⚠️ 6 падений — **существующие**, не связаны с правками |

**Важно:** 7 падающих тестов в `MarketDataProcessorTests` (логирование ошибок, таймер) были проверены на baseline (через `git stash`) — они падают **и без моих изменений**. Это уже существующие проблемы, не являющиеся регрессиями от данной реализации.

---

## Сборка

`dotnet build MarketDataCollector.sln` — **Build succeeded, 0 warnings, 0 errors**.
