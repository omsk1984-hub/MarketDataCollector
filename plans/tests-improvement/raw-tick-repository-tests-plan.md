# План улучшения: RawTickRepositoryTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate` — не проверяет результат

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:191)

**Описание:** Тест проверяет:
```csharp
result.All(t => t.Timestamp >= now.AddDays(-1.5));
```

Это выражение возвращает `bool`, но не используется с `Should().BeTrue()`. **Проверка не выполняется.** Тест проходит только потому, что других проверок достаточно (проверка `HaveCount(2)`).

**Необходимо:** Добавить `.Should().BeTrue()`:
```csharp
result.All(t => t.Timestamp >= now.AddDays(-1.5)).Should().BeTrue();
```

### Проблема 2: Тест `GetByTickerAsync_ReturnsTicksOrderedByTimestamp` — нестабилен из-за DateTime.UtcNow

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:272)

**Описание:** Тест создаёт тики с `DateTime.UtcNow`, `DateTime.UtcNow.AddDays(-1)`, `DateTime.UtcNow.AddDays(-2)`. Из-за быстрого выполнения теста все три `DateTime.UtcNow` могут быть очень близки (в пределах миллисекунд), и порядок сортировки может быть недетерминированным.

**Необходимо:** Использовать заранее вычисленные значения времени:
```csharp
var now = DateTime.UtcNow;
var ticks = new List<RawTick>
{
    new RawTick("BTCUSDT", 1002.00m, 0.2m, now, "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1000.50m, 0.5m, now.AddDays(-2), "Binance", new SystemTimeService()),
    new RawTick("BTCUSDT", 1001.00m, 0.3m, now.AddDays(-1), "Binance", new SystemTimeService())
};
```

### Проблема 3: Тест `GetByTickerAsync_ReturnsTicksOrderedByTimestamp` — неверная проверка последнего элемента

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:294)

**Описание:** Тест проверяет:
```csharp
result.Last().Timestamp.Should().Be(DateTime.UtcNow);
```

`DateTime.UtcNow` в момент проверки может отличаться от `DateTime.UtcNow` в момент создания тика. Даже небольшая разница в миллисекундах приведёт к провалу теста.

**Необходимо:** Сохранить значение `DateTime.UtcNow` в переменную при создании тиков и использовать её для проверки.

### Проблема 4: Тест `ExistsAsync_WhenTickDoesNotExist_ReturnsFalse` — не добавляет тик в БД

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:367)

**Описание:** Тест проверяет, что тик с `differentTimestamp` не существует. Но в БД нет никаких тиков (тест не добавляет их). Тест проверяет, что `ExistsAsync` возвращает `false` для пустой БД. Это корректно, но не проверяет, что `ExistsAsync` различает существующие и несуществующие тики.

**Необходимо:** Добавить тик в БД и проверить, что `ExistsAsync` возвращает `false` для другого timestamp.

### Проблема 5: Тест `FindAsync_WithPredicate_ReturnsMatchingTicks` — не проверяет предикат

**Файл:** [`tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Repositories/RawTickRepositoryTests.cs:464)

**Описание:** Тест проверяет `result.All(t => t.Ticker == "BTCUSDT").Should().BeTrue()`. Это корректно. Однако не проверяется, что тики с другими тикерами не попали в результат.

**Необходимо:** Проверка уже достаточна.

### Рекомендации:

1. **Исправить отсутствующую `.Should().BeTrue()`** в тесте `GetByTickerAsync_WithFromDate_ReturnsTicksFromThatDate`.
2. **Исправить нестабильность `DateTime.UtcNow`** в тесте `GetByTickerAsync_ReturnsTicksOrderedByTimestamp`.
3. **Добавить тест на `GetByExchangeAsync` с фильтрацией по датам** — проверить, что `from` и `to` работают.
4. **Добавить тест на `GetCountAsync` с пустой БД** — должно возвращать 0.
5. **Добавить тест на `Update` с несуществующим тиком** — проверка поведения.
6. **Добавить тест на `Remove` с несуществующим тиком** — проверка, что не выбрасывает исключение.