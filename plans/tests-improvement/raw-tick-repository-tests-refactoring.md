# План рефакторинга: RawTickRepositoryTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. Отсутствует `.Should().BeTrue()` на строке 214 — проверка `All()` не выполняется
2. Нестабильный `DateTime.UtcNow` в `GetByTickerAsync_ReturnsTicksOrderedByTimestamp` — при быстром выполнении теста все времена могут совпадать, и порядок не гарантирован
3. Проверка `result.Last().Timestamp.Should().Be(DateTime.UtcNow)` (строка 294) крайне нестабильна — разница в миллисекундах приводит к провалу
4. `ExistsAsync_WhenTickDoesNotExist_ReturnsFalse` не добавляет тик в БД, поэтому проверяет пустую БД, а не отличие существующего от несуществующего

**Изменения исходного кода не требуются** — все проблемы только в тестовом файле.

## Пошаговый план

### Шаг 1: Добавить `.Should().BeTrue()` в тест `GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:191)

**Проблема:** Строка 214: `result.All(t => t.Timestamp >= now.AddDays(-1.5));` — это выражение вычисляется, но его результат `bool` нигде не проверяется. Тест проходит только благодаря проверке `HaveCount(2)` на строке 213, но это не гарантирует правильности фильтрации.

**Решение:** Добавить `.Should().BeTrue()`:

```csharp
// Было (строка 214):
result.All(t => t.Timestamp >= now.AddDays(-1.5));

// Стало:
result.All(t => t.Timestamp >= now.AddDays(-1.5)).Should().BeTrue();
```

### Шаг 2: Исправить нестабильность `DateTime.UtcNow` в `GetByTickerAsync_ReturnsTicksOrderedByTimestamp`

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:272)

**Проблема:** Три вызова `DateTime.UtcNow` (строки 278-280) могут дать одинаковое время при быстром выполнении. Тест ожидает порядок по возрастанию, а если все времена равны — порядок недетерминирован.

**Решение:** Использовать заранее вычисленные значения времени с явными интервалами:

```csharp
// Было (строки 276-281):
var ticks = new List<RawTick>
{
    new RawTick("BTCUSDT", 1002.00m, 0.2m, DateTime.UtcNow, "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow.AddDays(-2), "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1001.00m, 0.3m, DateTime.UtcNow.AddDays(-1), "Binance", new SystemTimeService())
};

// Стало:
var now = DateTime.UtcNow;
var ticks = new List<RawTick>
{
    new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService())
};
```

### Шаг 3: Исправить нестабильную проверку последнего элемента по времени

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:294)

**Проблема:** `DateTime.UtcNow` проверки (строка 294) может отличаться от `DateTime.UtcNow` создания тика (строка 278) на миллисекунды, что приведёт к провалу теста.

**Решение:** Использовать сохранённую переменную `now` для проверки:

```csharp
// Было (строки 293-294):
result.First().Timestamp.Should().Be(DateTime.UtcNow.AddDays(-2));
result.Last().Timestamp.Should().Be(DateTime.UtcNow);

// Стало:
result.First().Timestamp.Should().Be(now.AddDays(-2));
result.Last().Timestamp.Should().Be(now);
```

### Шаг 4: Исправить `ExistsAsync_WhenTickDoesNotExist_ReturnsFalse` — добавить тик в БД

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:367)

**Проблема:** Тест проверяет `ExistsAsync` на пустой БД, что не проверяет различие существующего и несуществующего тика.

**Решение:** Добавить тик в БД, затем проверить, что тик с другим timestamp не существует:

```csharp
// Было (строки 370-377):
// Arrange
var differentTimestamp = DateTime.UtcNow.AddSeconds(1);

// Act
var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", differentTimestamp);

// Assert
exists.Should().BeFalse();

// Стало:
// Arrange
var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", new SystemTimeService());
await _repository.AddAsync(tick);
await _repository.SaveChangesAsync();

var differentTimestamp = DateTime.UtcNow.AddSeconds(1);

// Act
var exists = await _repository.ExistsAsync("BTCUSDT", "Binance", differentTimestamp);

// Assert
exists.Should().BeFalse();
```

### Шаг 5 (Опционально): Вынести создание тиков во вспомогательный метод

**Описание:** Многие тесты повторяют один и тот же код для создания списка тиков. Можно вынести в helper-метод:

```csharp
private async Task<List<RawTick>> SeedTicksAsync(params (string Ticker, decimal Price, decimal Volume, DateTime Timestamp, string Exchange)[] ticks)
{
    var result = new List<RawTick>();
    foreach (var (ticker, price, volume, timestamp, exchange) in ticks)
    {
        var tick = new RawTick(ticker, price, volume, timestamp, exchange, new SystemTimeService());
        await _repository.AddAsync(tick);
        result.Add(tick);
    }
    await _repository.SaveChangesAsync();
    return result;
}
```

### Шаг 6 (Опционально): Убрать избыточный `foreach` с `AddAsync`

**Описание:** Во всех тестах используется паттерн:
```csharp
foreach (var tick in ticks) { await _repository.AddAsync(tick); }
await _repository.SaveChangesAsync();
```
Можно заменить на `AddRangeAsync`.

## Итоговый список изменений

| # | Файл | Изменение | Тип |
|---|------|-----------|-----|
| 1 | `tests/.../RawTickRepositoryTests.cs` | Добавить `.Should().BeTrue()` в `GetByTickerAsync_WithFromDate` | Исправление |
| 2 | `tests/.../RawTickRepositoryTests.cs` | Заменить `DateTime.UtcNow` на переменную `now` в `ReturnsTicksOrderedByTimestamp` | Исправление |
| 3 | `tests/.../RawTickRepositoryTests.cs` | Исправить проверку `Last().Timestamp` на использование сохранённой `now` | Исправление |
| 4 | `tests/.../RawTickRepositoryTests.cs` | Добавить тик в БД в `ExistsAsync_WhenTickDoesNotExist_ReturnsFalse` | Улучшение |
| 5 | `tests/.../RawTickRepositoryTests.cs` | (Опционально) Вынести создание тиков в helper-метод | Рефакторинг |
| 6 | `tests/.../RawTickRepositoryTests.cs` | (Опционально) Заменить `foreach+AddAsync` на `AddRangeAsync` | Рефакторинг |
