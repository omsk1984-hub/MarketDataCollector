# План рефакторинга: SubscriptionManagerTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Проблемы незначительные. Основные улучшения:
1. Уточнить комментарии в тесте `RetryDelayIsExponential`
2. Добавить явный флаг `actionCalled` в тест `CancellationToken_CancelsOperation`
3. Убрать избыточные `VerifyAll` / проверить `Times.Exactly`

## Пошаговый план

### Шаг 1: Уточнить комментарии в тесте `SubscribeWithRetryAsync_RetryDelayIsExponential`

**Проблема:** Комментарий вводит в заблуждение: `retryAttempt=1 → 2^1 = 2s`.

**Решение:** Уточнить комментарий, указав, что `retryAttempt` в Polly начинается с 1, и задержка = `TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))`.

```csharp
// Заменить комментарий (строка 277):
// На:
// Задержка вычисляется как TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
// где retryAttempt в Polly начинается с 1:
// retryAttempt=1 → 2^1 = 2s, retryAttempt=2 → 2^2 = 4s, retryAttempt=3 → 2^3 = 8s
```

### Шаг 2: Добавить `actionCalled` флаг в тест `SubscribeWithRetryAsync_CancellationToken_CancelsOperation`

**Проблема:** Тест косвенно проверяет, что `subscribeAction` не вызван — через отсутствие логов.

**Решение:** Добавить прямой флаг `actionCalled` для явной проверки, что делегат не выполнился.

```csharp
[Fact(Timeout = 5000)]
public async Task SubscribeWithRetryAsync_CancellationToken_CancelsOperation()
{
    // Arrange
    var actionCalled = false;
    
    var subscribeAction = new Func<string, CancellationToken, Task>(async (symbol, ct) =>
    {
        actionCalled = true; // Этот код не должен выполниться
        await Task.Delay(100, ct);
    });

    var manager = new SubscriptionManager(
        _connectionManagerMock.Object,
        Options.Create(_defaultOptions),
        _loggerMock.Object,
        subscribeAction);

    var symbol = "BTCUSDT";
    using var cts = new CancellationTokenSource();
    cts.Cancel();

    // Act & Assert
    var act = async () => await manager.SubscribeWithRetryAsync(symbol, cts.Token);
    await act.Should().ThrowAsync<OperationCanceledException>();
    
    // Прямая проверка, что subscribeAction НЕ вызывался
    actionCalled.Should().BeFalse();
    
    // Косвенная проверка через логи
    _loggerMock.Verify(
        x => x.Log(
            LogLevel.Warning,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Попытка подписки")),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        Times.Never);
}
```

### Шаг 3: Убрать избыточные `VerifyAll` / обобщить проверки (опционально)

**Проблема:** В тестах используется множество `_loggerMock.Verify` с полной сигнатурой `Log`. Это делает тесты хрупкими — любое изменение форматирования лога сломает тест.

**Решение (опционально):** Вынести проверку лога во вспомогательный метод:

```csharp
// Добавить helper-метод в класс тестов:
private void VerifyLogContains(LogLevel level, string containsText, Func<Times> times)
{
    _loggerMock.Verify(
        x => x.Log(
            level,
            It.IsAny<EventId>(),
            It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains(containsText)),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
        times);
}
```

## Итоговый список изменений

| # | Файл | Изменение |
|---|------|-----------|
| 1 | `tests/.../SubscriptionManagerTests.cs` | Уточнить комментарий в `RetryDelayIsExponential` |
| 2 | `tests/.../SubscriptionManagerTests.cs` | Добавить `actionCalled` флаг в `CancellationToken_CancelsOperation` |
| 3 | `tests/.../SubscriptionManagerTests.cs` | (Опционально) Добавить helper для проверки логов |