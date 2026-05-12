# План улучшения: DataStorageServiceTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `StoreRawTickAsync_LogsDebugMessage` — неверная проверка лога

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs:74)

**Описание:** Тест проверяет, что лог содержит "Raw tick stored". Однако в реальной реализации [`DataStorageService.StoreRawTickAsync`](src/MarketDataCollector.Application/Services/DataStorageService.cs:23) сообщение лога выглядит так:
```csharp
_logger.LogDebug("Raw tick stored: {Id} {Ticker} {Price}", rawTick.Id, rawTick.Ticker, rawTick.Price);
```

Форматированное сообщение будет содержать "Raw tick stored: {GUID} BTCUSDT 1000.50". Тест проверяет `o.ToString()!.Contains("Raw tick stored")` — это корректно, так как сообщение начинается с "Raw tick stored".

**Вывод:** Тест корректен.

### Проблема 2: Тест `StoreRawTicksBatchAsync_LogsBatchSize` — неверная проверка лога

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs:182)

**Описание:** Тест проверяет, что лог содержит "Batch of". В реальной реализации [`StoreRawTicksBatchAsync`](src/MarketDataCollector.Application/Services/DataStorageService.cs:39):
```csharp
_logger.LogDebug("Batch of {Count} raw ticks stored", rawTicks);
```

Обратите внимание: в `LogDebug` передаётся **сам `rawTicks` (IEnumerable)**, а не `rawTicks.Count()`. При форматировании `{Count}` будет вызван `ToString()` на `IEnumerable<RawTick>`, что даст не число, а строку типа `System.Collections.Generic.List`1[MarketDataCollector.Domain.Entities.RawTick]`. **Это баг в исходном коде, а не в тесте!**

Тест проверяет `o.ToString()!.Contains("Batch of")` — это проходит, так как сообщение начинается с "Batch of". Но само сообщение будет содержать не число, а тип коллекции.

**Необходимо:** 
1. Исправить исходный код: заменить `rawTicks` на `rawTicks.Count()` или `rawTicks.Count()`.
2. Обновить тест для проверки корректного числа.

### Проблема 3: Тест `StoreRawTicksBatchAsync_LogsBatchSize` — не проверяет число

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs:182)

**Описание:** Тест проверяет только наличие "Batch of" в сообщении, но не проверяет, что число тиков (2) отображается корректно. После исправления бага из Проблемы 2, нужно добавить проверку на число.

### Проблема 4: Тест `StoreRawTickAsync_WhenRepositoryThrows_LogsErrorAndRethrows` — не проверяет повторный выброс

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs:128)

**Описание:** Тест проверяет, что исключение выбрасывается (`Assert.ThrowsAsync`) и что лог содержит "Error storing raw tick". Это корректно. Однако не проверяется, что `SaveChangesAsync` НЕ был вызван после ошибки `AddAsync`. В реальной реализации `AddAsync` выбрасывает исключение, и `SaveChangesAsync` не вызывается.

**Необходимо:** Добавить проверку `_repositoryMock.Verify(x => x.SaveChangesAsync(...), Times.Never)`.

### Проблема 5: Отсутствуют тесты для методов `GetUnnormalizedTicksAsync` и `MarkAsNormalizedAsync`

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DataStorageServiceTests.cs)

**Описание:** В реальном [`DataStorageService`](src/MarketDataCollector.Application/Services/DataStorageService.cs:107) есть методы `GetUnnormalizedTicksAsync` и `MarkAsNormalizedAsync`, но для них нет тестов.

**Необходимо:** Добавить тесты для этих методов.

### Рекомендации:

1. **Исправить баг** в `DataStorageService.StoreRawTicksBatchAsync` — заменить `rawTicks` на `rawTicks.Count()`.
2. **Добавить проверку `Times.Never` для `SaveChangesAsync`** в тестах с ошибками репозитория.
3. **Добавить тесты для `GetUnnormalizedTicksAsync` и `MarkAsNormalizedAsync`**.
4. **Добавить тест на `StoreRawTicksBatchAsync` с пустой коллекцией**.