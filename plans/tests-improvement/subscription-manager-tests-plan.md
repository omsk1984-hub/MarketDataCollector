# План улучшения: SubscriptionManagerTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Тест `SubscribeWithRetryAsync_RetryDelayIsExponential` — неверная проверка задержек

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs:278)

**Описание:** Тест проверяет, что лог содержит сообщения с задержками `2, 4, 8` секунд. Однако в реальной реализации [`SubscriptionManager.SubscribeWithRetryAsync`](src/MarketDataCollector.Core/Clients/SubscriptionManager.cs:39) используется `Math.Pow(2, retryAttempt)`, где `retryAttempt` начинается с 1 (это поведение Polly). Это даёт задержки: 2¹=2с, 2²=4с, 2³=8с. **Фактически тест верен**, но комментарий вводит в заблуждение: `retryAttempt=1 → 2^1 = 2s` — это не `2^1`, а `2^1=2`. Всё корректно.

**Вывод:** Тест корректен, но комментарии стоит уточнить.

### Проблема 2: Тест `SubscribeWithRetryAsync_ThrowsException_RetriesUpToMax` — потенциально нестабилен

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs:126)

**Описание:** Тест использует мутабельный `retryCount` внутри `subscribeAction`. Polly вызывает `subscribeAction` несколько раз. Если Polly изменит поведение (например, количество retry-попыток), тест может стать нестабильным. Сейчас тест ожидает `retryCount.Should().Be(maxRetries + 1)` — это 4 вызова при `MaxSubscribeRetries=3`. Это корректно, так как Polly делает 1 начальный вызов + 3 retry = 4.

**Вывод:** Тест корректен.

### Проблема 3: Тест `SubscribeWithRetryAsync_CancellationToken_CancelsOperation` — не проверяет реальную отмену

**Файл:** [`tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/SubscriptionManagerTests.cs:196)

**Описание:** Тест отменяет `CancellationToken` до вызова `SubscribeWithRetryAsync`. Polly действительно выбрасывает `OperationCanceledException` до вызова делегата. Однако тест не проверяет, что `subscribeAction` НЕ был вызван — он проверяет только отсутствие логов. Это косвенная проверка.

**Вывод:** Тест корректен, но можно добавить явный флаг `actionCalled`.

### Рекомендации:

1. **Уточнить комментарии** в тесте `RetryDelayIsExponential` — указать, что `retryAttempt` в Polly начинается с 1.
2. **Добавить явный флаг `actionCalled`** в тест `CancellationToken_CancelsOperation` для прямой проверки, что делегат не вызывался.
3. **Добавить тест на `SubscribeWithRetryAsync` с проверкой передачи `CancellationToken`** в `subscribeAction`.