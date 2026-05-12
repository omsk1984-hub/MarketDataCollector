# План рефакторинга: MarketDataProcessorTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`
**Приоритет:** Критический (❌)

## Общая стратегия

Главные проблемы:
1. **Нестабильные `Task.Delay`** — тесты полагаются на ожидание по времени вместо детерминированного завершения
2. **Неверная дедупликация** — `SetupSequence` для `ExistsAsync` не работает, когда `GroupBy` схлопывает тики с одинаковым timestamp
3. **Нет проверок записи в канал** — тесты проверяют только логи, но не то, что данные попали в канал

## Пошаговый план

### Шаг 1: Исправить тест `ProcessTickAsync_WritesToChannel`

**Проблема:** Проверяет только `processor.Should().NotBeNull()`.

**Решение:** Проверить, что тик можно прочитать из канала. Для этого нужно сделать канал инъектируемым или использовать рефлексию. Рекомендуется сделать `Channel` доступным через protected свойство.

**Изменения в исходном коде (`MarketDataProcessor.cs`):**

```csharp
// Добавить в класс MarketDataProcessor:
protected internal Channel<TickData> Channel => _channel;
```

**Изменения в тесте:**

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessTickAsync_WritesToChannel()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 5,
        channelCapacity: 100);

    var ticker = "BTCUSDT";
    var price = 1000.50m;
    var volume = 0.5m;
    var timestamp = DateTime.UtcNow;
    var exchange = "Binance";

    // Act
    await processor.ProcessTickAsync(ticker, price, volume, timestamp, exchange);

    // Assert - читаем из канала
    var reader = processor.Channel.Reader;
    var tick = await reader.ReadAsync(TimeSpan.FromSeconds(1));
    tick.Ticker.Should().Be(ticker);
    tick.Price.Should().Be(price);
    tick.Volume.Should().Be(volume);
    tick.Exchange.Should().Be(exchange);
}
```

### Шаг 2: Исправить тест `ProcessTickAsync_LogsDebugMessage`

**Проблема:** Не проверяет запись в канал. Канал с `FullMode.Wait` может заблокироваться, если нет читателя.

**Решение:** Добавить читателя канала или использовать предварительно прочитанный тик.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessTickAsync_LogsDebugMessage()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 5,
        channelCapacity: 100);

    // Act
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Debug,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Тик добавлен в очередь")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);

    // Дополнительно: проверить, что тик в канале
    var tick = await processor.Channel.Reader.ReadAsync(TimeSpan.FromSeconds(1));
    tick.Should().NotBeNull();
}
```

### Шаг 3: Исправить тест `StartProcessingAsync_StartsBackgroundTask`

**Проблема:** Проверяет только `task.Should().NotBeNull()` и лог.

**Решение:** Добавить тик в канал до вызова `StartProcessingAsync` и проверить, что он был обработан.

```csharp
[Fact(Timeout = 10000)]
public async Task StartProcessingAsync_StartsBackgroundTask()
{
    // Arrange
    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 5,
        channelCapacity: 100);

    using var cts = new CancellationTokenSource();

    // Act
    var task = processor.StartProcessingAsync(cts.Token);

    // Assert
    task.Should().NotBeNull();
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Обработчик рыночных данных запущен")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);

    // Добавляем тик и проверяем, что он обработан
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");
    await processor.StopProcessingAsync(cts.Token);

    _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 4: Исправить тест `StopProcessingAsync_StopsProcessing`

**Проблема:** Использует `Task.Delay(50)`, не проверяет завершение канала и задачи.

**Решение:** Убрать `Task.Delay`, проверять завершение задачи.

```csharp
[Fact(Timeout = 10000)]
public async Task StopProcessingAsync_StopsProcessing()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 5,
        channelCapacity: 100);

    using var cts = new CancellationTokenSource();

    var processingTask = processor.StartProcessingAsync(cts.Token);

    // Act
    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Обработчик рыночных данных остановлен")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);

    // Проверяем, что задача обработки завершена
    processingTask.IsCompleted.Should().BeTrue();
}
```

### Шаг 5: Исправить тест `StopProcessingAsync_LogsProcessedCount`

**Проблема:** Дважды вызывает `StopProcessingAsync`.

**Решение:** Убрать двойной вызов.

```csharp
[Fact(Timeout = 10000)]
public async Task StopProcessingAsync_LogsProcessedCount()
{
    // Arrange
    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 5,
        channelCapacity: 100);

    using var cts = new CancellationTokenSource();

    await processor.StartProcessingAsync(cts.Token);
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");

    // Act
    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Всего обработано")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 6: Исправить тест `ProcessBatchAsync_SavesNewTicksToRepository`

**Проблема:** Использует `Task.Delay(200)`.

**Решение:** Убрать `Task.Delay`, использовать `StopProcessingAsync` для детерминированного ожидания.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_SavesNewTicksToRepository()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 2,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act
    var timestamp1 = DateTime.UtcNow.AddMinutes(-1);
    var timestamp2 = DateTime.UtcNow;
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
    await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, timestamp2, "Binance");

    // Детерминированное ожидание через StopProcessingAsync
    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
    _repositoryMock.Verify(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 7: Исправить тест `ProcessBatchAsync_SkipsDuplicateTicks`

**Проблема:** `SetupSequence` для `ExistsAsync` не работает, так как `GroupBy` может схлопнуть тики с одинаковым timestamp. Оба тика используют `DateTime.UtcNow`.

**Решение:** Использовать разные timestamp для тиков, чтобы они не схлопнулись в `GroupBy`.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_SkipsDuplicateTicks()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 2,
        channelCapacity: 100);

    // Используем разные timestamp, чтобы GroupBy не схлопнул их
    var timestamp1 = DateTime.UtcNow.AddMinutes(-1);
    var timestamp2 = DateTime.UtcNow;

    // Первый тик не существует, второй - существует (дубликат)
    _repositoryMock.SetupSequence(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false)  // Первый тик не существует
        .ReturnsAsync(true);  // Второй тик существует

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp2, "Binance"); // Дубликат по содержимому, но разный timestamp

    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Once);
}
```

### Шаг 8: Исправить тест `ProcessBatchAsync_LogsTotalProcessedEvery100`

**Проблема:** `Task.Delay(500)`, все 100 тиков с `DateTime.UtcNow` могут схлопнуться.

**Решение:** Использовать разные timestamp для всех 100 тиков и детерминированное ожидание.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_LogsTotalProcessedEvery100()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 1,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act - добавляем 100 тиков с разными timestamp
    var baseTime = DateTime.UtcNow;
    for (int i = 0; i < 100; i++)
    {
        await processor.ProcessTickAsync("BTCUSDT", 1000.50m + i, 0.5m, baseTime.AddMilliseconds(i), "Binance");
    }

    // Детерминированное ожидание
    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Information,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Всего обработано тиков")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.AtLeastOnce);
}
```

### Шаг 9: Исправить тест `ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent`

**Проблема:** `Task.Delay(200)`.

**Решение:** Убрать `Task.Delay`, использовать `StopProcessingAsync`.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 2,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    _repositoryMock.Setup(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()))
        .ThrowsAsync(new InvalidOperationException("Database error"));

    var errorOccurred = false;
    processor.OnError += (sender, ex) =>
    {
        errorOccurred = true;
        ex.Should().NotBeNull();
    };

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act
    var timestamp1 = DateTime.UtcNow.AddMinutes(-1);
    var timestamp2 = DateTime.UtcNow;
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
    await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, timestamp2, "Binance");

    await processor.StopProcessingAsync(cts.Token);

    // Assert
    errorOccurred.Should().BeTrue();
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Error,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Критическая ошибка")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 10: Исправить тест `ProcessBatchAsync_LogsSavedCount`

**Проблема:** `Task.Delay(200)`.

**Решение:** Аналогично Шагу 6.

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_LogsSavedCount()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 2,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act
    var timestamp1 = DateTime.UtcNow.AddMinutes(-1);
    var timestamp2 = DateTime.UtcNow;
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
    await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, timestamp2, "Binance");

    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Debug,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Батч сохранён")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);
}
```

### Шаг 11: Добавить тест на пустой батч (все дубликаты)

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_AllDuplicates_LogsWarning()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 2,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(true);

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    // Act
    var timestamp1 = DateTime.UtcNow.AddMinutes(-1);
    var timestamp2 = DateTime.UtcNow;
    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, timestamp1, "Binance");
    await processor.ProcessTickAsync("BTCUSDT", 1001.00m, 0.3m, timestamp2, "Binance");

    await processor.StopProcessingAsync(cts.Token);

    // Assert
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Debug,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("дубликатами")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Once);

    // AddRangeAsync не должен вызываться
    _repositoryMock.Verify(x => x.AddRangeAsync(It.IsAny<IEnumerable<RawTick>>(), It.IsAny<CancellationToken>()), Times.Never);
}
```

### Шаг 12: Добавить тест на отмену через CancellationToken

```csharp
[Fact(Timeout = 10000)]
public async Task ProcessBatchAsync_CancellationDuringProcessing_StopsGracefully()
{
    // Arrange
    var processor = new MarketDataProcessor(
        _repositoryMock.Object,
        _loggerMock.Object,
        _timeServiceMock.Object,
        batchSize: 1,
        channelCapacity: 100);

    _repositoryMock.Setup(x => x.ExistsAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
        .ReturnsAsync(false);

    using var cts = new CancellationTokenSource();
    await processor.StartProcessingAsync(cts.Token);

    await processor.ProcessTickAsync("BTCUSDT", 1000.50m, 0.5m, DateTime.UtcNow, "Binance");

    // Act - отменяем токен
    cts.Cancel();

    // Assert - StopProcessingAsync должен завершиться без исключения
    await processor.StopProcessingAsync(CancellationToken.None);
}
```

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs` | Добавить `protected internal Channel<TickData> Channel => _channel;` |
| 2 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessTickAsync_WritesToChannel` — читать из канала |
| 3 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessTickAsync_LogsDebugMessage` — добавить проверку канала |
| 4 | `tests/.../MarketDataProcessorTests.cs` | Исправить `StartProcessingAsync_StartsBackgroundTask` — добавить проверку обработки |
| 5 | `tests/.../MarketDataProcessorTests.cs` | Исправить `StopProcessingAsync_StopsProcessing` — убрать `Task.Delay` |
| 6 | `tests/.../MarketDataProcessorTests.cs` | Исправить `StopProcessingAsync_LogsProcessedCount` — убрать двойной вызов |
| 7 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessBatchAsync_SavesNewTicksToRepository` — убрать `Task.Delay` |
| 8 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessBatchAsync_SkipsDuplicateTicks` — разные timestamp |
| 9 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessBatchAsync_LogsTotalProcessedEvery100` — разные timestamp, убрать `Task.Delay` |
| 10 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndRaisesEvent` — убрать `Task.Delay` |
| 11 | `tests/.../MarketDataProcessorTests.cs` | Исправить `ProcessBatchAsync_LogsSavedCount` — убрать `Task.Delay` |
| 12 | `tests/.../MarketDataProcessorTests.cs` | Добавить тест на пустой батч (все дубликаты) |
| 13 | `tests/.../MarketDataProcessorTests.cs` | Добавить тест на отмену через CancellationToken |