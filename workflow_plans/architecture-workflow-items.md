# Список пунктов архитектурных улучшений

Сформирован в рамках воркфлоу `architecture-improvements`.
Источник: `plans/architecture-review-report.md`, `plans/architecture-fix-execution-plan.md`.
Статус пункта: `[ ]` не выполнен, `[x]` выполнен (одобрено ревью).

---

## Пункт 1. Вынести Kafka за абстракцию `ICandlePublisher` и убрать `Application → Infrastructure`

- Проблема: `TickAggregator` напрямую использует `using MarketDataCollector.Infrastructure.Kafka;` и конкретный тип `KafkaCandleProducer?`. Это единственная ссылка `Application → Infrastructure` в `Application.csproj`, нарушающая DIP.
- Целевой результат (DoD): в `Application/` нет `using MarketDataCollector.Infrastructure.*`; `Application.csproj` не ссылается на Infrastructure; проект собирается; Kafka-тесты проходят.
- Затрагиваемые файлы: `src/MarketDataCollector.Application/Services/TickAggregator.cs`, `src/MarketDataCollector.Application/MarketDataCollector.Application.csproj`, `src/MarketDataCollector.Core/Interfaces/ICandlePublisher.cs` (новый), `src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs`, `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs`, `tests/.../KafkaIntegrationTests.cs`.
- Риски: изменение DI-композиции, правка сигнатур в тестах Kafka.

Статус: [ ]

---

## Пункт 2. Очистить Domain от EF-атрибутов и Npgsql, перенести маппинг в Fluent API

- Проблема: доменные сущности `RawTick`, `ConnectionLog`, `AggregatedData` помечены EF/DataAnnotations-атрибутами, а пакет `Npgsql.EntityFrameworkCore.PostgreSQL` подключён в `Domain.csproj`. Доменный слой привязан к БД/ORM.
- Целевой результат (DoD): из `Domain.csproj` удалён `Npgsql.EFCore`; сущности — чистые POCO без атрибутов; весь маппинг перенесён в `MarketDataDbContext.OnModelCreating` (Fluent API); схема БД не изменилась (миграция/тесты это подтверждают).
- Затрагиваемые файлы: `src/MarketDataCollector.Domain/MarketDataCollector.Domain.csproj`, `src/MarketDataCollector.Domain/Entities/RawTick.cs`, `src/MarketDataCollector.Domain/Entities/ConnectionLog.cs`, `src/MarketDataCollector.Domain/Entities/AggregatedData.cs`, `src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs`.
- Риски: изменение схемы БД, ошибки маппинга свойств/индексов.

Статус: [ ]

---

## Пункт 3. Убрать Npgsql-специфичную обработку из Application

- Проблема: `MarketDataProcessor` напрямую `using Npgsql;` и ловит `NpgsqlException`, т.е. прикладной слой знает про конкретную СУБД.
- Целевой результат (DoD): в `Application/` отсутствует `using Npgsql.*`; обработка ошибок БД абстрагирована в репозиторий/Infrastructure; Application работает с типизированными доменными исключениями/результатами.
- Затрагиваемые файлы: `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`, репозитории `src/MarketDataCollector.Infrastructure/Repositories/*`, интерфейсы Core при необходимости.
- Риски: изменение контракта обработки ошибок, затронет тесты `MarketDataProcessorTests`.

Статус: [ ]

---

## Пункт 4. Убрать технологическую лексику из интерфейсов Core

- Проблема: `IRawTickRepository.BulkCopyAsync` несёт Npgsql COPY-семантику, т.е. контракт привязан к конкретной технологии.
- Целевой результат (DoD): контракт переименован в нейтральный (`BulkInsertFastAsync` / `BatchInsertAsync`); технологическая лексика убрана из интерфейсов Core; реализации и вызовы обновлены; тесты проходят.
- Затрагиваемые файлы: `src/MarketDataCollector.Core/Interfaces/IRawTickRepository.cs`, реализации `src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs`, вызывающий код, тесты.
- Риски: широкое распространение вызовов, требуется аккуратный рефакторинг.

Статус: [ ]

---

## Пункт 5. Удалить или подключить мёртвый `DataStorageService`

- Проблема: `DataStorageService` и `IDataStorageService` не зарегистрированы в DI и не используются — мёртвый код, дублирующий `IRawTickRepository`.
- Целевой результат (DoD): сервис либо удалён вместе с интерфейсом, либо подключён в реальный пайплайн; в коде не остаётся неиспользуемых обёрток.
- Затрагиваемые файлы: `src/MarketDataCollector.Application/Services/DataStorageService.cs` (и интерфейс внутри), при необходимости DI в `Program.cs`/`DependencyInjection.cs`.
- Риски: минимальный, но нужно убедиться в отсутствии скрытых использований.

Статус: [ ]

---

> Новые пункты, обнаруженные в ходе конвейера, добавляются в конец списка с тем же форматом.
