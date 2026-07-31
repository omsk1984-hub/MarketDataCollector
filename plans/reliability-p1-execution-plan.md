# План: Надёжность (P1) — миграции, партиционирование, Kafka RF, логгер, алерты

Дата: 2026-07-31
Статус: план СОГЛАСОВАН (Architect), решения приняты — передан на реализацию
База: `plans/production-readiness-assessment.md` (раздел «Надёжность (P1)»)

## Принятые решения (согласованы 2026-07-31)
- **Kafka:** self-hosted 3-нодный кластер (KRaft) в `docker-compose.prod.yml`; dev-композ остаётся single-node.
- **Партиционирование:** hosted-сервис в воркере (`PartitionMaintenanceService`) — без внешних расширений Postgres.
- **Миграции:** авто `Database.Migrate()` при старте Worker (не отдельный шаг деплоя).
- **`init.sql`:** оставить в dev (`docker-compose.yml`); в prod (`docker-compose.prod.yml`) схему создают миграции.

## Контекст / текущее состояние

| Задача | Текущее состояние | Где |
|--------|-------------------|-----|
| EF Core миграции | Отсутствуют. Схема создаётся только `docker/init.sql` при первом старте Postgres. В `Infrastructure.csproj` нет пакетов Design/Tools. | [`init.sql`](docker/init.sql:1), [`MarketDataDbContext`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs:16), [`Infrastructure.csproj`](src/MarketDataCollector.Infrastructure/MarketDataCollector.Infrastructure.csproj:9) |
| Партиционирование `RawTicks` | Только ручной `init-partitioned.sql` с жёстко зашитыми датами (`2026_07_29`…), не подключён к compose, не автоматизирован. Вставка идёт raw SQL `INSERT INTO rawticks ... ON CONFLICT (ticker, exchange, timestamp)`. | [`init-partitioned.sql`](docker/init-partitioned.sql:21), [`RawTickRepository`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:31) |
| Kafka RF | dev и prod-композ single-node, `replication-factor 1`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`. Топики создаются `kafka-init-topics`. | [`docker-compose.yml`](docker/docker-compose.yml:51), [`docker-compose.prod.yml`](docker/docker-compose.prod.yml:44) |
| `Console.WriteLine` в `VerifyKafkaAvailability` | Три вызова `Console.WriteLine` вместо логгера. | [`DependencyInjection.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/DependencyInjection.cs:146) |
| Мониторинг-алерты | Есть метрики OpenTelemetry (`ChannelBacklog`, `TicksDroppedSilently`, `ActiveConnections`, `TicksDropped`), Prometheus (scrape `/metrics`), `/health`. Нет alerting rules, нет Alertmanager. | [`MarketDataTelemetry.cs`](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs:156), [`prometheus.yml`](docker/prometheus/prometheus.yml:1), [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:89) |

---

## 1. EF Core миграции

**Цель:** управление схемой БД через миграции вместо ручного `init.sql`, вызов `Database.Migrate()` при старте/деплое.

### Шаги
1. В `Infrastructure.csproj` добавить:
   - `Microsoft.EntityFrameworkCore.Design` (private assets).
   - (для запуска миграций из CLI) `.Design` достаточно; Tools ставится как глобальный dotnet-инструмент.
2. Добавить класс `MarketDataCollector.Infrastructure` → `DesignTimeDbContextFactory` (реализация `IDesignTimeDbContextFactory<MarketDataDbContext>`), читающий строку подключения из env `ConnectionStrings__MarketDataDb` или `MarketDataDb__Default`, чтобы `dotnet ef migrations add` работал без запуска Worker.
3. Сгенерировать начальную миграцию `InitialCreate` (схема из Fluent API `OnModelCreating`): `dotnet ef migrations add InitialCreate`.
4. **Точка применения миграций:**
   - **Рекомендуемый вариант — отдельный шаг деплоя** (не авто-миграция из Worker): добавить в `deploy.yml` перед `up -d` запуск однократного контейнера `dotnet ef database update` (или `dotnet ef migrations bundle`). Это исключает гонки при нескольких репликах Worker.
   - **Альтернатива — авто-миграция** `Database.Migrate()` при старте Worker (проще, но риск гонок при масштабировании).
5. Решить судьбу `init.sql`:
   - В prod (compose.prod + deploy) **убрать** монтирование `init.sql` — схему создают миграции.
   - В dev (docker-compose.yml) можно оставить `init.sql` как bootstrap, но переключить на миграции для консистентности.

### Критично
- Схема в Fluent API и `init.sql` **должны совпадать** с миграцией. После генерации `InitialCreate` проверить, что имена таблиц/колонок (snake_case) и ограничения совпадают.
- Учесть партиционирование (раздел 2): начальная миграция может сразу создавать `RawTicks` как партиционированную таблицу, либо миграции добавляют партиционирование отдельным шагом.

---

## 2. Автоматизация партиционирования `RawTicks`

**Цель:** автоматическое создание партиций на будущее и удаление/архив старых без ручного продления дат.

### Ключевое ограничение (технический долг)
- EF-модель [`RawTick`](src/MarketDataCollector.Domain/Entities/RawTick.cs) имеет PK `Id`. Для **native-партиционирования по `Timestamp`** в Postgres PK обязан включать partition key → PK `(Id, Timestamp)`.
- [`RawTickRepository`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:31) вставляет raw SQL `INSERT INTO rawticks (...) ON CONFLICT (ticker, exchange, timestamp)`. Партиционирование по `Timestamp` потребует `ON CONFLICT` с учётом partition key и корректной вставки в партиции.
- `init-partitioned.sql` уже показывает схему с PK `(Id, Timestamp)` и unique-индексом `(Ticker, Exchange, Timestamp)`.

### Два варианта реализации

**Вариант А — pg_partman + pg_cron (рекомендуется, без кода):**
- Добавить расширения `pg_partman` и `pg_cron` в Postgres (образ `postgres:16-alpine` — нужны дополнительные пакеты/расширение; возможно отдельный init-контейнер или включение расширений через SQL при старте).
- `create_parent` для `rawticks` по `timestamp`, интервал `1 day`, `premake 7`.
- `run_maintenance()` по расписанию pg_cron (например, каждый час).
- Retention: настроить `retention = '30 days'`, `retention_keep_table = false`.
- Изменение PK: выполнить в миграции (раздел 1) — пересоздать таблицу с PK `(Id, Timestamp)`.

**Вариант Б — hosted-сервис/cron в воркере (без внешних расширений):**
- Добавить `BackgroundService` (например `PartitionMaintenanceService`), который раз в интервал (например, час) выполняет SQL:
  - `CREATE TABLE IF NOT EXISTS rawticks_YYYY_MM_DD PARTITION OF rawticks FOR VALUES FROM (...) TO (...)` на N дней вперёд.
  - `DROP TABLE IF EXISTS rawticks_YYYY_MM_DD` для партиций старше retention (с настраиваемым периодом).
- Не требует внешних расширений, проще для деплоя, но дублирует логику, которую умеет pg_partman.

### Рекомендация
Выбрать **Вариант А (pg_partman)** как основной (проверенное решение), оставив Вариант Б как fallback, если хочется избежать внешних расширений в Postgres. Решение согласовать с пользователем (см. вопросы в конце).

### Миграция данных
Если на проде уже есть данные в непартиционированной `rawticks`:
1. Создать партиционированную таблицу `rawticks_partitioned` с PK `(Id, Timestamp)`.
2. `INSERT INTO rawticks_partitioned SELECT * FROM rawticks;`
3. `ALTER TABLE rawticks RENAME TO rawticks_old; ALTER TABLE rawticks_partitioned RENAME TO rawticks;`
4. `DROP TABLE rawticks_old;`
Это оформить как EF-миграцию (Raw SQL в `MigrationBuilder.Sql`).

---

## 3. Кластер Kafka с RF ≥ 3

**Цель:** отказоустойчивость Kafka — отсутствие потери данных/оффсетов при падении ноды.

### Варианты
**Вариант 1 — управляемый сервис (Confluent Cloud / MSK / Redpanda Cloud):**
- Минимальные эксплуатационные затраты, RF ≥ 3 из коробки, managed.
- Для этого проекта (запуск на целевом сервере через compose) — менее подходит, но для реального продакшена оптимален.
- Требует обновления `KafkaOptions`/`.env` (адреса, TLS/SASL), подключение секретов.

**Вариант 2 — self-hosted 3-нодный кластер (KRaft) в compose.prod:**
- Развернуть `kafka1/kafka2/kafka3` с `KAFKA_NODE_ID 1/2/3`, `KAFKA_PROCESS_ROLES: broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093`.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2`, `KAFKA_DEFAULT_REPLICATION_FACTOR: 3`.
- В `kafka-init-topics`: `--replication-factor 3` для `raw-ticks`, `aggregated-data`, `connection-events`.
- Worker подключается к `kafka1:9092,kafka2:9092,kafka3:9092`.
- Учесть `min.insync.replicas` и конфиг продюсера/консюмера в `KafkaProducer.cs`/`KafkaCandleConsumerService.cs` (ackс).

### Критично
- В dev-`docker-compose.yml` оставить single-node (для локальной разработки), в `docker-compose.prod.yml` — кластер RF 3.
- Обновить `.env.example` (`KAFKA_BOOTSTRAP_SERVERS` → список брокеров), `deploy.yml`.
- Проверить поведение `VerifyKafkaAvailability` и fallback на прямую запись в БД при частичной недоступности кластера.

---

## 4. Заменить `Console.WriteLine` на логгер в `VerifyKafkaAvailability`

**Текущее:** [`VerifyKafkaAvailability`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/DependencyInjection.cs:133) — статический метод, вызывается в [`Program.cs:74`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:74) **до** построения DI-контейнера (доступ к `ILogger` невозможен через DI на этом этапе).

### План
- Вынести проверку в hosted-сервис (`KafkaAvailabilityCheckService : BackgroundService`) или в начало `Worker.ExecuteAsync`, где доступен `ILogger<...>` через DI.
- Либо: добавить параметр `ILoggerFactory` / `ILogger` в `VerifyKafkaAvailability`. Поскольку метод вызывается до `builder.Build()`, получить `ILogger` можно через `services.BuildServiceProvider()` (создать ранний провайдер) или через `LoggerFactory.Create(...)`.
- Заменить все `Console.WriteLine` на `_logger.LogInformation/LogWarning` с структурированными параметрами.
- Сохранить поведение «не бросаем исключений; при недоступности Kafka свечи fallback в БД», но сигнализировать оператору через метрику/лог.

### Критично
- Не терять раннюю проверку: она должна выполняться до старта агрегации (`AddAggregation`), либо проверка переносится в старт агрегатора. Сохранить порядок инициализации.

---

## 5. Мониторинг-алерты

**Цель:** алерты на backlog, дропы, дисконнекты WebSocket, недоступность `/health`.

### Что уже есть
- Метрики OTel: `processor.channel.backlog`, `ticks.dropped.silently`, `ticks.dropped`, `ws.active_connections`, `exceptions_total`, `/metrics` (Prometheus), `/health` (Kafka, Postgres, WebSocket, channels).
- Prometheus scraping worker через `host.docker.internal:5010`.

### План
1. Создать `docker/prometheus/rules.yml` с alerting rules:
   - **Backlog:** `processor.channel.backlog > 50000` (порог согласовать с ёмкостью канала 150000).
   - **Дропы:** rate `ticks.dropped.silently` / `ticks.dropped` за 5m > порога.
   - **Дисконнекты WS:** `ws.active_connections` < ожидаемого числа клиентов ИЛИ снижение до 0 в течение N минут.
   - **Здоровье:** blackbox/статус `/health` — через Prometheus blackbox exporter (HTTP probe на `/health`, статус != 200 → alert), либо через промайнторинг `up`.
   - **Kafka недоступен / Postgres недоступен** — по `/health` (см. выше) или по `up`.
2. Подключить `prometheus.yml` к `rules.yml`.
3. Добавить `alertmanager` в compose (dev и prod) с конфигом уведомлений (e-mail / webhook).
4. Обновить `Program.cs`/метрики при необходимости (например, экспонировать статус `/health` как метрику gauge для простого алерта без blackbox).

### Критично
- Пороги должны быть откалиброваны под реальную нагрузку (~19–25K msg/s), чтобы не было ложных срабатываний. Учесть пики старта (stagger 2s).
- Алерты не должны зависеть от Availability (5 9) — важно отсутствие шумных alerts.

---

## 6. Обновление конфигурации и деплоя

- `docker/.env.example`: `KAFKA_BOOTSTRAP_SERVERS` → список брокеров, добавить пороги алертов/retention, настройки alertmanager.
- `docker/docker-compose.prod.yml`: Kafka-кластер RF 3 (или ссылка на managed), подключить `rules.yml` + `alertmanager`, убрать `init.sql` в пользу миграций.
- `docker/docker-compose.yml` (dev): оставить single-node Kafka, но добавить `rules.yml`/alertmanager для локальной проверки алертов.
- `.github/workflows/deploy.yml`: шаг миграций (`dotnet ef database update` / migrations bundle) перед `up -d`; добавить копирование `rules.yml`, `alertmanager` конфиг; обновить `.env` генерацию.
- `DEPLOYMENT.md`: документировать процесс деплоя с миграциями и алертами.

---

## Порядок выполнения (todo)
1. [ ] EF Core миграции: пакеты, `DesignTimeDbContextFactory`, `InitialCreate`, точка применения (деплой-шаг или авто).
2. [ ] Согласовать `init.sql` и миграции (убрать конфликт; судьба `init.sql`).
3. [ ] Партиционирование: вариант А (pg_partman+pg_cron) или Б (hosted-сервис); миграция PK `(Id, Timestamp)`; миграция данных при необходимости.
4. [ ] Retention/архивация партиций (настраиваемый период).
5. [ ] Kafka RF ≥ 3: managed или 3-node self-hosted; обновить продюсер/консюмер.
6. [ ] Заменить `Console.WriteLine` на логгер в `VerifyKafkaAvailability`.
7. [ ] Мониторинг-алерты: `rules.yml`, Alertmanager, blackbox.
8. [ ] Обновить `.env.example`, compose (dev+prod), `deploy.yml`, `DEPLOYMENT.md`.
9. [ ] Обновить `production-readiness-assessment.md` (отметить выполненные P1).

---

## Вопросы на согласование (перед реализацией)

1. **Kafka:** управляемый сервис (Confluent/MSK/Redpanda Cloud) или self-hosted 3-нодный кластер в compose.prod?
2. **Партиционирование:** pg_partman+pg_cron (расширения Postgres) или hosted-сервис в воркере (без внешних расширений)?
3. **Применение миграций:** отдельный шаг деплоя (`dotnet ef database update`) или авто-миграция `Database.Migrate()` при старте Worker?
4. **Судьба `init.sql`:** удалить в prod в пользу миграций (рекомендуется) или оставить как bootstrap?
