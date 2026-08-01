# Перепроверка тестов после изменений по плану add-dedup-metrics

## Контекст

Изменения по [`add-dedup-metrics-plan.md`](plans/add-dedup-metrics-plan.md) добавляют две метрики:
- [`TicksDeduplicatedByCache`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:84) — `ticks.deduplicated.cache`
- [`TicksDeduplicatedByDb`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:93) — `ticks.deduplicated.db`

Запись в `ProcessBatchAsync` (строки 914-927). Изменения затрагивают ТОЛЬКО блок успешной обработки и
НЕ трогают логирование и не влияют на тесты.

## Результат прогона

- Passed: 239
- Failed: 9
- Прогон прерван крашем тест-хоста (`ObjectDisposedException` в `CollectorLoopAsync`).

## Вывод: ни одно из 9 падений НЕ вызвано изменениями add-dedup-metrics

Новые метрики корректно компилируются и инкрементируются в успешном пути. Тесты на метрики
дедупликации отсутствуют (поиск `DeduplicatedByCache|deduplicated.cache|deduplicated.db` в тестах — 0 совпадений),
поэтому новые изменения не имеют тестового покрытия.

## Классификация падений

### A. Предсуществующие «мёртвые» тесты на лог-сообщения (5 шт.)

Проверяют сообщения, которых НЕТ в [`MarketDataProcessor.Logging.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.Logging.cs).

| Тест | Ожидает | Реальность |
|---|---|---|
| ProcessBatchAsync_LogsSkippedDuplicates (стр. 383) | Debug «дубликатов» | нет такого LoggerMessage |
| ProcessBatchAsync_LogsSavedCount (стр. 470) | Debug «Батч сохранён» | нет такого LoggerMessage |
| ProcessBatchAsync_LogsTotalProcessedEvery100 (стр. 695) | Debug «Батч сохранён» | нет такого LoggerMessage |
| ProcessBatchAsync_DbException_ContinuesProcessing (стр. 636) | Error «Критическая ошибка» ×2 | LogUnexpectedBatchError |
| ProcessBatchAsync_WhenRepositoryThrows_LogsErrorAndConsumerContinues (стр. 420) | Error «Критическая ошибка» | LogUnexpectedBatchError |

### B. Дефект в обработчике — краш тест-хоста

[`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:424):
`flushTimerCts` в `using`, но `Timer`-колбэк может сработать после Dispose →
`ObjectDisposedException: The CancellationTokenSource has been disposed`. Роняет весь прогон.

Отдельно тест FlushTimer_MultipleTimerTicks_OnlyOneFlush (стр. 1306) падает по таймингу: callCount=1 вместо ≥2.

### C. Падение кэша дедупликации

[`DeduplicationCache.Add`](src/MarketDataCollector.Application/Services/DeduplicationCache.cs:80):
эвикция только при `_addCount % EvictionCheckInterval(100) == 0`. Для теста
Add_EvictsOldest_WhenMaxSizeReached (maxSize=3, 4 добавления) эвикция не срабатывает → ts1 не вытесняется.

## Рекомендации

1. (Критично) Защитить Timer-колбэк в CollectorLoopAsync от вызова после Dispose — устранит краш хоста.
2. Решить судьбу 5 «мёртвых» тестов: обновить под реальные LoggerMessage или удалить.
3. Решить судьбу DeduplicationCache: либо тест, либо эвикция при каждом Add при превышении лимита.
4. Добавить unit-тест на новые метрики TicksDeduplicatedByCache/Db (покрытие отсутствует).
