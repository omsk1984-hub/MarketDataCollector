# Реализация: исправление падающих тестов + покрытие метрик дедупликации

## Результат

До правок: **239 passed / 9 failed / прогон прерван крашем тест-хоста**.
После правок: **256 passed / 0 failed / Skipped: 0 — ВСЕ тесты проходят.**

## Дополнительно: тестовая конфигурация Kafka

Добавлен отдельный [`appsettings.json`](tests/MarketDataCollector.Tests/appsettings.json) для
тестового проекта с `Kafka.Enabled=true` (в общем appsettings Worker'а Kafka выключен).
`.csproj` подключён к копированию локального файла вместо Worker-файла. Это устранило
4 инфраструктурных падения `KafkaRealConnectionTests`.

## Выполненные изменения

### 1. Краш таймера в CollectorLoopAsync (`MarketDataProcessor.cs`)
Колбэк `Timer` вызывал `flushTimerCts.Cancel()` после `Dispose` из `using`-блока →
`ObjectDisposedException`, ронявший тест-хост. Обёрнуто в try/catch(ObjectDisposedException).
Устранён краш всего прогона.

### 2. Актуализация «мёртвых» тестов на логи
- `MarketDataProcessor.Logging.cs`: добавлены Debug-`LoggerMessage` `LogBatchSaved` (EventId 19,
  «Батч сохранён») и `LogBatchDeduplicated` (EventId 20, «дубликатов»).
- `MarketDataProcessor.cs`: вызовы `LogBatchSaved`/`LogBatchDeduplicated` в `ProcessBatchAsync`
  после успешной записи.
- Тесты, ожидавшие несуществующее Error-сообщение «Критическая ошибка», переведены на реальное
  `LogUnexpectedBatchError` («Неожиданная ошибка при обработке батча»).
- `FlushTimer_MultipleTimerTicks_OnlyOneFlush`: добавлен `MinPartialBatchSize=1`, чтобы тимерный
  сброс маленьких частичных батчей (3 и 2 тика) реально срабатывал.

### 3. Расхождение DeduplicationCache (`DeduplicationCache.cs`)
Эвикция срабатывала только раз в `EvictionCheckInterval` (100) добавлений, из-за чего кэш превышал
`maxSize`. Заменено на эвикцию при каждом превышении лимита (пакетно, 10% за раз).
Удалены неиспользуемые `_addCount` и `EvictionCheckInterval`.

### 4. Тесты на метрики дедупликации (`DeduplicationMetricsTests.cs`)
Добавлены unit-тесты через `MeterListener` на глобальный `MarketDataTelemetry.Instance`:
- `TicksDeduplicatedByCache_Exists_And_Accumulates` (ticks.deduplicated.cache)
- `TicksDeduplicatedByDb_Exists_And_Accumulates` (ticks.deduplicated.db)

## Kafka-тесты (устранены тестовой конфигурацией)

Ранее 4 теста `KafkaRealConnectionTests` падали, т.к. читали общий `appsettings.json` Worker'а
с `Kafka.Enabled=false`. Исправлено добавлением отдельного тестового [`appsettings.json`](tests/MarketDataCollector.Tests/appsettings.json)
с `Enabled=true` и подключением его в `.csproj`. Тесты поднимают собственный Kafka-контейнер
через Testcontainers, поэтому требуют Docker. Итог — все 256 тестов проходят.
