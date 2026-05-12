# План улучшения: MonitoringServiceTests.cs

**Файл:** `tests/MarketDataCollector.Tests/Application/Services/MonitoringServiceTests.cs`

## Результат проверки: ✅ Тесты корректны

Все тесты в данном файле корректны и соответствуют реализации.

### Проверенные сценарии:

| Тест | Статус |
|------|--------|
| `Constructor_CreatesService` | ✅ |
| `StartMonitoring_StartsStatusTimer` | ✅ |
| `StopMonitoring_StopsStatusTimer` | ✅ |
| `UpdateConnectionStatus_UpdatesStatusToConnected` | ✅ |
| `UpdateConnectionStatus_UpdatesStatusToDisconnected` | ✅ |
| `UpdateConnectionStatus_UpdatesStatusToError` | ✅ |
| `UpdateConnectionStatus_LogsConnectedEvent` | ✅ |
| `UpdateConnectionStatus_LogsDisconnectedEvent` | ✅ |
| `UpdateConnectionStatus_LogsErrorEvent` | ✅ |
| `UpdateConnectionStatus_LogsErrorMessage` | ✅ |
| `IncrementTickCounter_IncrementsCounter` | ✅ |
| `IncrementTickCounter_IncrementsCounterMultipleTimes` | ✅ |
| `IncrementTickCounter_IncrementsTotalCount` | ✅ |
| `IncrementTickCounter_DifferentExchangesHaveSeparateCounters` | ✅ |
| `GetConnectionStatus_WhenNotSet_ReturnsDisconnected` | ✅ |
| `GetTickCount_WhenNotSet_ReturnsZero` | ✅ |
| `GetTotalTicksProcessed_ReturnsZeroInitially` | ✅ |
| `ResetCounters_ResetsTickCounters` | ✅ |
| `ResetCounters_LogsResetEvent` | ✅ |
| `StartMonitoring_CanBeCalledMultipleTimes` | ✅ |
| `StopMonitoring_CanBeCalledMultipleTimes` | ✅ |

### Рекомендации (необязательные улучшения):

1. **Добавить тест на `StartMonitoring` с проверкой, что таймер действительно запущен** — можно проверить через вызов `LogStatus` (но он приватный). Альтернативно, проверить, что после `StartMonitoring` и ожидания >30с лог содержит "=== Monitoring Status ===". Однако это сделает тест медленным.
2. **Добавить тест на потокобезопасность `IncrementTickCounter`** — параллельные вызовы из нескольких потоков.
3. **Добавить тест на `UpdateConnectionStatus` с пустым сообщением** — проверка, что сообщение не добавляется в лог.