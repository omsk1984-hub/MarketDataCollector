# План: замена fire-and-forget агрегатора на TryWrite (вариант B)

## Проблема

В `MarketDataProcessor.ProcessTickAsync` (`src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:197`) агрегатор свечей вызывается как fire-and-forget:

```csharp
_ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
```

Проблемы:
- `OnTickAsync` возвращает `Task`, который игнорируется через `_ =`.
- Создаётся лишний Task + state machine на каждый тик в hot path (аллокации).
- При закрытом/переполненном канале возможны необработанные исключения.

## Цель (вариант B)

Заменить асинхронный `OnTickAsync` на **синхронный `TryWriteTick`**, который пишет в `Channel` через `ChannelWriter.TryWrite` и возвращает `bool`. Это убирает:
- fire-and-forget,
- аллокации Task/state machine в hot path,
- риск необработанных исключений (TryWrite не бросает, возвращает `false`).

## Изменения

### 1. Интерфейс `src/MarketDataCollector.Core/Interfaces/ITickAggregator.cs`

Заменить метод:
```csharp
Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
```
на:
```csharp
/// <summary>
/// Передать тик в агрегатор (неблокирующая запись в канал).
/// Возвращает true, если тик принят, false — если канал переполнен/закрыт.
/// </summary>
bool TryWriteTick(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange);
```

### 2. Реализация `src/MarketDataCollector.Application/Services/TickAggregator.cs`

Заменить `OnTickAsync` (строки 132-136):
```csharp
public Task OnTickAsync(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
{
    if (!_enabled) return Task.CompletedTask;
    return _channel.Writer.WriteAsync(new TickData(ticker, price, volume, timestamp, exchange)).AsTask();
}
```
на:
```csharp
public bool TryWriteTick(string ticker, decimal price, decimal volume, DateTime timestamp, string exchange)
{
    if (!_enabled) return true; // отключено — «принято» по семантике
    return _channel.Writer.TryWrite(new TickData(ticker, price, volume, timestamp, exchange));
}
```

### 3. Вызов в `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`

Заменить (строки 195-198):
```csharp
if (_tickAggregator != null)
{
    _ = _tickAggregator.OnTickAsync(ticker, price, volume, timestamp, exchange);
}
```
на:
```csharp
_tickAggregator?.TryWriteTick(ticker, price, volume, timestamp, exchange);
```

### 4. Тесты `tests/MarketDataCollector.Tests/Application/Services/TickAggregatorTests.cs`

Заменить все `await aggregator.OnTickAsync(...)` на `aggregator.TryWriteTick(...)`.
Внимание: тесты полагаются на попадание тика в канал и последующую обработку через `StopAsync`/таймер. `TryWrite` синхронный — тик попадает в канал сразу, семантика сохраняется.
- Добавить/обновить тест: `TryWriteTick_WhenChannelClosed_ReturnsFalse` (edge case — закрытый канал возвращает `false`, не бросает).

### 5. Тесты `tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`

- Все mock-и `aggregatorMock.Setup(x => x.OnTickAsync(...)).Returns(Task.CompletedTask)` → `.Setup(x => x.TryWriteTick(...)).Returns(true)`.
- `slowAggregatorMock.Setup(x => x.OnTickAsync(...)).Returns(async () => await Task.Delay(5000))` → `.Setup(x => x.TryWriteTick(...)).Returns(true)`. Тест `ProcessTickAsync_DoesNotBlockWhenAggregatorIsSlow` перестанет быть осмысленным (TryWrite синхронный) — обновить/упростить (проверка остаётся: `ProcessTickAsync` не блокируется).
- `Verify(x => x.OnTickAsync(...))` → `Verify(x => x.TryWriteTick(...))`.

### 6. Тесты `tests/MarketDataCollector.Tests/Infrastructure/Kafka/KafkaIntegrationTests.cs`

Заменить `await aggregator.OnTickAsync(...)` на `aggregator.TryWriteTick(...)`.

## Верификация

1. `dotnet build MarketDataCollector.sln` — компиляция без ошибок.
2. `dotnet test` для `MarketDataCollector.Tests` — все тесты зелёные (happy path + edge cases).
3. Проверить, что нет оставшихся упоминаний `OnTickAsync` в src (кроме, возможно, устаревших ссылок).

## Риски

- `TryWrite` при полном канале (`FullMode = DropOldest`) дропает старый элемент и возвращает `true`. Семантика DropOldest сохраняется.
- Тест `ProcessTickAsync_DoesNotBlockWhenAggregatorIsSlow` требует пересмотра (агрегатор больше не может быть «медленным» через интерфейс).
