# План рефакторинга: DataStorageServiceTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. **Баг в исходном коде:** `StoreRawTicksBatchAsync` передаёт сам `rawTicks` (IEnumerable) вместо `rawTicks.Count()` в `LogDebug`
2. Не проверяется, что `SaveChangesAsync` НЕ вызывается после ошибки `AddAsync`
3. Отсутствуют тесты для методов `GetUnnormalizedTicksAsync` и `MarkAsNormalizedAsync`

## Пошаговый план

### Шаг 1: Исправить баг в исходном коде (`DataStorageService.cs`)

**Проблема:** В `DataStorageService.StoreRawTicksBatchAsync` (строка 45):
```csharp
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks);
```
Передаётся `rawTicks` (IEnumerable), а не количество. При форматировании `{Count}` будет выведен `ToString()` коллекции.

**Решение:** Заменить `rawTicks` на `rawTicks.Count()`.

```csharp
// В src/MarketDataCollector.Application/Services/DataStorageService.cs, строка ~45
// Было:
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks);

// Стало:
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks.Count());
```

**Важно:** Убедиться, что `using System.Linq;` присутствует в файле (обычно есть).

### Шаг 2: Исправить тест `StoreRawTicksBatchAsync_LogsBatchSize`

**Проблема:** Тест проверяет только `Contains("Batch of")`, но не проверяет число.

**Решение:** После исправления бага в исходном коде, добавить проверку на число:

```csharp
[Fact(Timeout = 10000)]
public async Task StoreRawTicksBatchAsync_LogsBatchSize()
{
    // ... существующий код Arrange ...

    // Act & Assert
    // После исправления бага, Count будет корректно отображаться
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Debug,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => 
                o.ToString()!.Contains("Batch of") && 
                o.ToString()!.Contains("2")), // Проверяем, что число 2 отображается
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 3: Исправить тест `StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows`

**Проблема:** Не проверяется, что `SaveChangesAsync` не вызывается после ошибки.

**Решение:** Добавить проверку `Times.Never`.

```csharp
[Fact(Timeout = 10000)]
public async Task StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows()
{
    // ... существующий код Arrange ...

    // Act & Assert
    var act = async () => await service.StoreRawTickAsync(rawTick);
    await act.Should().ThrowAsync<Exception>();

    // Проверяем, что SaveChangesAsync НЕ вызывался
    _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Never);

    // Проверяем логирование ошибки
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Error storing raw tick")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 4: Добавить тест для `GetUnnormalizedTicksAsync`

**Поведение из исходного кода** ([`DataStorageService.GetUnnormalizedTicksAsync`](src/MarketDataCollector.Application/Services/DataStorageService.cs:107)):
- Вызывает `_rawTickRepository.FindAsync(t => !t.IsNormalized)`
- Ограничивает результат `limit` (по умолчанию 1000)
- Возвращает `IEnumerable<RawTick>`

```csharp
[Fact(Timeout = 5000)]
public async Task GetUnnormalizedTicksAsync_CallsRepositoryWithCorrectPredicate()
{
    // Arrange
    var ticks = new List<RawTick>
    {
        new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", _timeServiceMock.Object),
        new RawTick("ETHUSDT", 2500.75m, 1.0m, DateTime.UtcNow.AddMinutes(-1), "Binance", _timeServiceMock.Object)
    };

    _repositoryMock.Setup(x => x.FindAsync(
            It.Is<System.Linq.Expressions.Expression<Func<RawTick, bool>>>(expr => true),
            It.IsAny<CancellationToken>()))
        .ReturnsAsync(ticks);

    var service = new DataStorageService(_repositoryMock.Object, _loggerMock.Object, _timeServiceMock.Object);

    // Act
    var result = await service.GetUnnormalizedTicksAsync(limit: 1000);

    // Assert
    result.Should().HaveCount(2);
    _repositoryMock.Verify(x => x.FindAsync(
        It.IsAny<System.Linq.Expressions.Expression<Func<RawTick, bool>>>(),
        It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 5: Добавить тест для `MarkAsNormalizedAsync`

**Поведение из исходного кода** ([`DataStorageService.MarkAsNormalizedAsync`](src/MarketDataCollector.Application/Services/DataStorageService.cs:120)):
- Находит `RawTick` по `tickId`
- Если найден, устанавливает `IsNormalized = true`
- Сохраняет изменения
- Если не найден — ничего не делает

```csharp
[Fact(Timeout = 5000)]
public async Task MarkAsNormalizedAsync_WhenTickExists_MarksAsNormalized()
{
    // Arrange
    var tickId = Guid.NewGuid();
    var tick = new RawTick("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance", _timeServiceMock.Object);
    
    // Используем рефлексию для установки Id, если Id read-only
    // В зависимости от реализации RawTick, может потребоваться другой подход

    _repositoryMock.Setup(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()))
        .ReturnsAsync(tick);

    var service = new DataStorageService(_repositoryMock.Object, _loggerMock.Object, _timeServiceMock.Object);

    // Act
    await service.MarkAsNormalizedAsync(tickId);

    // Assert
    _repositoryMock.Verify(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()), Times.Once);
    _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
}

[Fact(Timeout = 5000)]
public async Task MarkAsNormalizedAsync_WhenTickNotFound_DoesNothing()
{
    // Arrange
    var tickId = Guid.NewGuid();

    _repositoryMock.Setup(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()))
        .ReturnsAsync((RawTick?)null);

    var service = new DataStorageService(_repositoryMock.Object, _loggerMock.Object, _timeServiceMock.Object);

    // Act
    await service.MarkAsNormalizedAsync(tickId);

    // Assert
    _repositoryMock.Verify(x => x.GetByIdAsync(tickId, It.IsAny<CancellationToken>()), Times.Once);
    _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Never);
}
```

### Шаг 6: Добавить тест на `StoreRawTicksBatchAsync` с пустой коллекцией

```csharp
[Fact(Timeout = 5000)]
public async Task StoreRawTicksBatchAsync_WithEmptyCollection_DoesNothing()
{
    // Arrange
    var service = new DataStorageService(_repositoryMock.Object, _loggerMock.Object, _timeServiceMock.Object);

    // Act
    await service.StoreRawTicksBatchAsync(new List<RawTick>());

    // Assert
    _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Never);
    _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Never);
}
```

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `src/.../DataStorageService.cs` | Исправить баг: заменить `rawTicks` на `rawTicks.Count()` в LogDebug |
| 2 | `tests/.../DataStorageServiceTests.cs` | Исправить `StoreRawTicksBatchAsync_LogsBatchSize` — добавить проверку числа |
| 3 | `tests/.../DataStorageServiceTests.cs` | Исправить `StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows` — добавить `Times.Never` для SaveChangesAsync |
| 4 | `tests/.../DataStorageServiceTests.cs` | Добавить тест `GetUnnormalizedTicksAsync_CallsRepositoryWithCorrectPredicate` |
| 5 | `tests/.../DataStorageServiceTests.cs` | Добавить тест `MarkAsNormalizedAsync_WhenTickExists_MarksAsNormalized` |
| 6 | `tests/.../DataStorageServiceTests.cs` | Добавить тест `MarkAsNormalizedAsync_WhenTickNotFound_DoesNothing` |
| 7 | `tests/.../DataStorageServiceTests.cs` | Добавить тест `StoreRawTicksBatchAsync_WithEmptyCollection_DoesNothing` |