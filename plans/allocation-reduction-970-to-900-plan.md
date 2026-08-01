# План снижения аллокаций с ~970 до ~900 байт/тик (возврат к эталону)

## Контекст

Прогон [`counters-analysis_20260801_133135.md`](plans/counters-analysis_20260801_133135.md) (строка 170):
- **~970 байт/тик** аллокаций — выше эталона 112029 (**~908**), дельта **~60 байт/тик (~6%)**.
- Рост **не прогрессирует** и **не вызывает потерь** (0 дропов, backlog ~1.9, 2 Gen2, GC 60.7ms).
- Это **единственное** оставшееся направление оптимизации hot-path.

Цель — вернуть аллокации к ~900–910 байт/тик **без регресса** дропов, backlog, Gen2, GC duration и throughput.

---

## Статус уже выполненных оптимизаций (baseline — не трогаем без причины)

Все приоритеты прошлого плана [`allocation-reduction-improvements-plan.md`](plans/allocation-reduction-improvements-plan.md) **уже внедрены** и подтверждены в коде:

| Оптимизация | Файл | Статус |
|---|---|---|
| Интернирование ticker (P1) | [`BinanceWebSocketClient.cs:158`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:158) | ✅ работает (fake-сервер шлёт `s` в верхнем регистре, набор `{BTCUSDT,ETHUSDT,SOLUSDT}` полностью покрыт) |
| Хэш-кэш `DedupKey` + `TryAdd` (P2) | [`DeduplicationCache.cs:25`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:25) | ✅ внедрён |
| `MemoryStream` → `ArrayBufferWriter` (P3) | [`WebSocketMessageReceiver.cs:84`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:84) | ✅ внедрён |
| Bulk-вставка на кэшированных массивах + UUID v7 | [`RawTickRepository.cs:457`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:457) | ✅ внедрён |
| Батчевые счётчики (`CounterBatcher`, zero-alloc) | [`CounterBatcher.cs:43`](src/MarketDataCollector.Core/Telemetry/CounterBatcher.cs:43) | ✅ внедрён |
| Кэшированные теги `ChannelTag`/`GetExchangeTag` | [`MarketDataProcessor.cs:64`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:64) | ✅ внедрён |

**Вывод:** поскольку интернирование ticker и остальные P-приоритеты уже в коде, а аллокации всё равно 970 (не 900), источник дельты — **не string ticker**, а **нечто иное per-тик**. Кандидаты ниже.

---

## Корневой анализ: что осталось аллоцировать на каждый тик

### Источник №1 (главный): async state machine в `ProcessMessageAsync`

[`BinanceWebSocketClient.cs:79`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:79):

```csharp
protected override async Task ProcessMessageAsync(ReadOnlyMemory<byte> message)
{
    try
    {
        var parsed = ParseTradeMessage(message.Span);          // синхронно
        if (parsed.IsTrade && parsed.Ticker != null)
        {
            var timestamp = ...;                               // синхронно
            await _dataProcessor.ProcessTickAsync(...);        // await синхронно завершённого Task
        }
    }
    catch (JsonException ex) { OnErrorOccurred(ex); }
    catch (Exception ex) { OnErrorOccurred(ex); }
}
```

**Проблема:** метод объявлен `async Task` и вызывается на **каждое WS-сообщение** (21K/сек). Хотя всё тело синхронно, а `ProcessTickAsync` возвращает уже завершённый `Task.CompletedTask` (см. [`MarketDataProcessor.cs:167`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:167)), компилятор всё равно генерирует async state machine и **боксует её в heap-`Task` (~40–80 байт) на каждый вызов**. Это и есть фиксированный per-тик расход, не закрытый интернированием.

**Оценка:** ~40–80 байт/тик — основной кандидат на дельту 970 vs 908.

### Источник №2 (CPU, не аллокация): `SlidingWindowCounter.Increment()`

[`BaseWebSocketClient.cs:399`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:399) вызывает `DateTimeOffset.UtcNow.ToUnixTimeSeconds()` на каждое сообщение — CPU, аллокаций нет. **Не трогаем** (приоритет низкий, выигрыша в байтах нет).

### Источник №3 (CPU, не аллокация): `DateTimeOffset.FromUnixTimeMilliseconds(...)`

[`BinanceWebSocketClient.cs:89`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:89) — вычисляется на каждый тик, аллокаций нет. **Не трогаем** для целей снижения байт/тик.

---

## План изменений

### Приоритет 1 (основной): убрать async-боксинг из `ProcessMessageAsync`

**Файл:** [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:79)

**Изменение:** сделать метод **не-async**, возвращающим `Task.CompletedTask`, и **не await'ить** `ProcessTickAsync` (он полностью синхронен — пишет в Channel и возвращает завершённый `Task`).

```csharp
protected override Task ProcessMessageAsync(ReadOnlyMemory<byte> message)
{
    try
    {
        var parsed = ParseTradeMessage(message.Span);

        if (parsed.IsTrade && parsed.Ticker != null)
        {
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(parsed.TradeTimeMs).UtcDateTime;
            // Не await'им: ProcessTickAsync синхронен (TryWrite в Channel), Task уже завершён.
            // Fire-and-forget безопасен — данные попали в Channel до возврата.
            _dataProcessor.ProcessTickAsync(parsed.Ticker, parsed.Price, parsed.Volume, timestamp, ExchangeName);
        }
    }
    catch (JsonException ex)
    {
        OnErrorOccurred(ex);
    }
    catch (Exception ex)
    {
        OnErrorOccurred(ex);
    }

    return Task.CompletedTask;
}
```

**Эффект:** −1 аллокация heap-`Task` (async state machine) на сообщение → **−40–80 байт/тик**, возврат к ~900.

**Проверки безопасности:**
- ✅ `ProcessTickAsync` синхронен до конца (Interlocked + `TryWrite` + fire-and-forget на `_tickAggregator.OnTickAsync`), никакого реального асинхронного I/O — данные гарантированно в Channel до возврата. Дроп при полном канале фиксируется `TryWrite` и инкрементит `_totalDroppedCount` синхронно.
- ✅ Интерфейс `IWebSocketMessageReceiver` ожидает `Func<ReadOnlyMemory<byte>, Task>` — сигнатура сохраняется.
- ✅ Базовый виртуальный [`BaseWebSocketClient.ProcessMessageAsync`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:274) тоже возвращает `Task` — override совместим.

**Примечание:** другие наследники (`KrakenWebSocketClient`, если есть аналогичный async-паттерн) — проверить и применить то же, если тело синхронно. В текущем дереве основной hot-path — Binance.

### Приоритет 2: проверить синхронность `OnTickAsync` при `Enabled: false`

[`MarketDataProcessor.cs:195`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:195):

```csharp
if (_tickAggregator != null)
{
    _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
}
```

[`TickAggregator.OnTickAsync`](src/MarketDataCollector.Application/Services/TickAggregator.cs:132) при `Enabled: false` возвращает `Task.CompletedTask` синхронно — **аллокаций нет**. При `Enabled: false` вызов бесполезен, но безвреден. **Опционально:** можно завернуть в `if (_tickAggregator != null && _tickAggregator.Enabled)` — микро-выигрыш CPU, но **не** влияет на байт/тик. Низкий приоритет.

### Приоритет 3: эмпирическое подтверждение (в code-режиме)

Разобрать `traces/allocation_trace_20260801_133135.nettrace` до/после правок:

```powershell
dotnet-trace report traces/allocation_trace_20260801_133135.nettrace analyze -profile-type GCAllocationTick
```

Цель — убедиться, что после правки исчезли аллокации `Task`/async state machine в стеке `ProcessMessageAsync → ProcessTickAsync`, и `System.String` per-тик остаётся нулевым (интернирование работает).

### Приоритет 4: контрольный замер

Повтор `run_all_metrics.ps1` (1.7M тиков, fake-сервер, DupPercent=3):

| Метрика | Текущее | Целевое |
|---|---|---|
| Аллокации | ~970 байт/тик | **≤ ~910** |
| Дропы | 0 | 0 (без регресса) |
| Backlog батчей | ~1.9 | ≈ (без регресса) |
| Gen2 сборок | 2 | ≤ 2 |
| GC duration | 60.7 ms | ≤ 70 ms |
| Эффективность записи | 97.01% | ≥ 97% |

---

## Критерии успеха

- Аллокации ≤ ~910 байт/тик (возврат к эталону ~908).
- Сохранение 0 дропов, backlog ≈ 2, эффективность записи ~97%.
- Без регресса Gen2 / GC duration / throughput.
- Проверено на 2+ прогонах для воспроизводимости.

---

## Порядок реализации

| # | Задача | Файл | Риск | Эффект |
|---|---|---|---|---|
| 1 | Убрать async из `ProcessMessageAsync` (не await'ить синхронный `ProcessTickAsync`) | [`BinanceWebSocketClient.cs:79`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:79) | низкий (сигнатура Task сохраняется) | **−40–80 байт/тик** |
| 2 | (Опционально) guard `Enabled` у `_tickAggregator.OnTickAsync` | [`MarketDataProcessor.cs:195`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:195) | низкий | CPU, не байты |
| 3 | Разбор `allocation_trace_20260801_133135.nettrace` | `traces/` (code-режим) | — | подтверждение |
| 4 | Контрольный замер `run_all_metrics.ps1` | `run_all_metrics.ps1` | — | подтверждение ~900 |

## Ключевые файлы
- [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:79) — устранение async-боксинга (основное)
- [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:195) — guard `_tickAggregator` (опционально)
- `traces/allocation_trace_20260801_133135.nettrace` — эмпирическое подтверждение
- `run_all_metrics.ps1` — контрольный замер
