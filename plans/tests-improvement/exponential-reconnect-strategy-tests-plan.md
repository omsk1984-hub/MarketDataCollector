# План улучшения: ExponentialReconnectStrategyTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Core/Clients/ExponentialReconnectStrategyTests.cs`

## Результат проверки: ✅ Тесты корректны

Все тесты в данном файле корректны и соответствуют реализации.

### Проверенные сценарии:

| Тест | Статус |
|------|--------|
| `Constructor_WithValidOptions_SetsProperties` | ✅ |
| `GetDelay_FirstAttempt_ReturnsBaseDelay` | ✅ |
| `GetDelay_SecondAttempt_ReturnsDoubleDelay` | ✅ |
| `GetDelay_ThirdAttempt_ReturnsExponentialDelay` | ✅ |
| `GetDelay_FourthAttempt_ReturnsExponentialDelay` | ✅ |
| `GetDelay_ExceedsMaxDelay_ReturnsCappedDelay` | ✅ |
| `GetDelay_WithLargeAttempt_ReturnsMaxDelay` | ✅ |
| `GetDelay_WithZeroAttempt_ThrowsArgumentOutOfRangeException` | ✅ |
| `GetDelay_WithNegativeAttempt_ThrowsArgumentOutOfRangeException` | ✅ |
| `ShouldRetry_AlwaysReturnsTrue` | ✅ |
| `Reset_LogsDebugMessage` | ✅ |

### Рекомендации (необязательные улучшения):

1. **Добавить тест на `GetDelay` с джиттером (jitter)** — если в будущем будет добавлен случайный разброс задержки, тесты нужно будет обновить.
2. **Рассмотреть параметризованные тесты (`[Theory]`)** для сокращения дублирования кода в тестах `GetDelay_FirstAttempt`..`FourthAttempt`.
