# План улучшения: MarketDataProcessorTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`

## Результат проверки: ❌ Серьёзные проблемы

### Проблема 1: Тест `ProcessTickAsync_WritesToChannel` — не проверяет запись в канал

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:59)

**Описание:** Тест вызывает `ProcessTickAsync` и затем проверяет только `processor.Should().NotBeNull()`. Это не проверяет, что тик действительно был записан в канал. В реальной реализации [`ProcessTickAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:56) пишет в `_channel.Writer.WriteAsync`. Тест должен проверить, что тик можно прочитать из канала.

**Необходимо:** Заменить проверку на чтение из канала или запуск обработки и проверку результата.

### Проблема 2: Тест `ProcessTickAsync_LogsDebugMessage` — не проверяет запись в канал

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:84)

**Описание:** Аналогично Проблеме 1 — проверяется только лог, но не то, что тик попал в канал. Лог пишется **после** записи в канал, поэтому если запись в канал не удалась (канал заполнен), лог всё равно будет записан (так как `WriteAsync` в `BoundedChannel` с `FullMode.Wait` будет ждать, но в тесте нет читателя, и канал может заблокироваться).

**Необходимо:** Добавить читателя канала или использовать `Channel.CreateBounded` с `FullMode.DropWrite` для тестов.

### Проблема 3: Тест `StartProcessingAsync_StartsBackgroundTask` — не проверяет запуск обработки

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:151)

**Описание:** Тест проверяет только `task.Should().NotBeNull()` и лог. Он не проверяет, что `ProcessBatchesAsync` действительно запущен и начал читать из канала. В реальной реализации [`StartProcessingAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:62) запускает `ProcessBatchesAsync`, который читает из канала.

**Необходимо:** Добавить тик в канал до вызова `StartProcessingAsync` и проверить, что он был обработан.

### Проблема 4: Тест `StopProcessingAsync_StopsProcessing` — не проверяет остановку

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:208)

**Описание:** Тест запускает обработку, ждёт 50мс, затем вызывает `StopProcessingAsync`. Проверяет только лог. Не проверяет, что:
- Канал завершён (`_channel.Writer.Complete()` вызван)
- Задача обработки завершена
- Все тики обработаны

**Необходимо:** Добавить проверки завершения канала и задачи.

### Проблема 5: Тест `StopProcessingAsync_LogsProcessedCount` — дважды вызывает `StopProcessingAsync`

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:239)

**Описание:** Тест вызывает `StopProcessingAsync` дважды (строки 254 и 257). Первый вызов завершает канал и ожидает обработки. Второй вызов снова завершает канал (что безопасно, `Complete()` можно вызывать多次) и снова логирует. Тест проверяет `Times.AtLeastOnce` для лога "Всего обработано". Это работает, но логика странная.

**Необходимо:** Убрать двойной вызов `StopProcessingAsync` или сделать отдельный тест для повторного вызова.

### Проблема 6: Тест `ProcessBatchAsync_SavesNewTicksToRepository` — нестабилен из-за `Task.Delay`

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:311)

**Описание:** Тест использует `Task.Delay(200)` для ожидания обработки батча. Это недетерминированно — на медленной машине 200мс может не хватить, на быстрой — слишком много. В реальной реализации `ProcessBatchesAsync` читает из канала и обрабатывает батчи асинхронно.

**Необходимо:** Использовать детерминированный подход: после вызова `StopProcessingAsync` дождаться завершения задачи обработки.

### Проблема 7: Тест `ProcessBatchAsync_SkipsDuplicateTicks` — неверная настройка `ExistsAsync`

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:342)

**Описание:** Тест использует `SetupSequence` для `ExistsAsync`:
```csharp
.SetupSequence(x => x.ExistsAsync(...))
    .ReturnsAsync(false)  // Первый тик не существует
    .ReturnsAsync(true);  // Второй тик существует
```

Однако в реальной реализации [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:136) сначала делается дедупликация в памяти через `GroupBy`, а затем проверка в БД через `GetExistingKeysFromDbAsync`, который вызывает `ExistsAsync` для каждого уникального тика. Если оба тика имеют одинаковые `(Ticker, Exchange, Timestamp)`, то после `GroupBy` останется только один уникальный тик, и `ExistsAsync` будет вызван только один раз.

В тесте оба тика имеют `DateTime.UtcNow` — они могут иметь одинаковый timestamp (в пределах миллисекунды), и `GroupBy` схлопнет их в один. Тогда `ExistsAsync` будет вызван только один раз, и `SetupSequence` с двумя значениями будет некорректен.

**Необходимо:** Использовать разные timestamp для тиков, чтобы они не схлопнулись в `GroupBy`.

### Проблема 8: Тест `ProcessBatchAsync_LogsTotalProcessedEvery100` — нестабилен

**Файл:** [`tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs:495)

**Описание:** Тест добавляет 100 тиков и ждёт 500мс. Это недетерминированно. В реальной реализации лог "Всего обработано тиков" выводится, когда `count % 100 < entities.Count`. Для 100 тиков с `batchSize=1` будет 100 батчей, и лог должен появиться (при count=100, 100%100=0 < 1). Но из-за `GroupBy` и проверки `ExistsAsync` некоторые тики могут быть отфильтрованы.

**Необходимо:** Использовать разные timestamp для всех 100 тиков и детерминированное ожидание через `StopProcessingAsync`.

### Рекомендации:

1. **Заменить `Task.Delay` на детерминированное ожидание** через `StopProcessingAsync` во всех тестах.
2. **Использовать разные timestamp** для тиков в тестах с дедупликацией.
3. **Добавить тест на `ProcessBatchAsync` с пустым батчем** (когда все тики — дубликаты).
4. **Добавить тест на отмену через CancellationToken** во время обработки.
5. **Добавить тест на переполнение канала** (channelCapacity=1, много тиков).
6. **Добавить тест на повторный запуск `StartProcessingAsync`** после остановки.