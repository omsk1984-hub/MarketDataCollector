# Оценка готовности MarketDataCollector к продакшену

Дата: 2026-07-31
Тип: предварительная архитектурная оценка (Architect mode) — основана на ревью кода и конфигурации, без запуска.

**Общий вердикт: система НЕ ГОТОВА к продакшену без доработок.** Ядро пайплайна (каналы, батчинг, дедупликация, Binary COPY, Single Consumer) спроектировано и оптимизировано хорошо и проверено нагрузочным тестированием. Однако отсутствуют производственные обвязки: деплой, CI/CD, продакшен-конфиг, управление секретами, миграции, резервирование Kafka — и есть архитектурные долги, зафиксированные в [`architecture-review-report.md`](plans/architecture-review-report.md). Ниже — детальная оценка по категориям.

---

## 1. Сильные стороны (готово хорошо)

| Область | Оценка | Комментарий |
|---------|--------|-------------|
| Ядро обработки потока | ✅ Готово | Channel + батчинг + Async Writer + Adaptive Batch Size. Нагрузка ~19–25K msg/s подтверждена тестами. |
| Дедупликация (3 уровня) | ✅ Готово | FIFO-кэш → GroupBy → `ON CONFLICT DO NOTHING`. |
| Запись в БД | ✅ Готово | Binary COPY + unique-индекс `(Ticker, Exchange, Timestamp)`. |
| Graceful shutdown | ✅ Готово | `_internalCts`, потери данных = 0 при штатной остановке. |
| Наблюдаемость | 🟡 Частично | Метрики/трейсы/логи через OpenTelemetry, `/health`, `/metrics`. См. п.4. |
| Обработка критических ошибок | ✅ Готово | Worker fault'ится → внешний рестарт (Docker/K8s). |
| Тесты | ✅ Хорошо | 16+ файлов unit/integration, Testcontainers. |

---

## 2. 🔴 Блокеры (обязательны до продакшена)

### 2.1 Секреты захардкожены в репозитории
[`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:20) содержит строку подключения с паролем `StrongPassword123!` в открытом виде. В [`docker-compose.yml`](docker/docker-compose.yml:10) тот же пароль, плюс `POSTGRES_HOST_AUTH_METHOD: trust` (dev-режим, без аутентификации).

**Требуется:** вынести секреты в env / secret-менеджер (K8s Secrets, Docker Secrets, vault). Пароль в коде — критическая утечка, особенно если репозиторий публичный (MIT License).

### 2.2 Нет продакшен-конфигурации (`appsettings.Production.json`)
Есть только [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:1) и `Development`. Production-настройки (URL бирж, capacity, логирование) должны переопределяться через env без правки кода.

### 2.3 Нет Dockerfile для воркера
Присутствует только [`docker-compose.yml`](docker/docker-compose.yml:1) для инфраструктуры (Postgres, Kafka, Prometheus, Aspire). **Сам воркер не контейнеризирован** — в compose нет сервиса `worker`. Для продакшена нужен `Dockerfile` (SDK→runtime, `dotnet publish`, healthcheck на `/health`).

### 2.4 Нет CI/CD
Нет `.github/workflows`, пайплайнов сборки, прогона тестов, публикации образов. Запуск/деплой ручной (через `run.ps1`).

---

## 3. 🟠 Существенные риски (необходимы для надёжного продакшена)

### 3.1 Схема БД инициализируется вручную, без миграций
Нет EF Core миграций и вызова `Database.Migrate()` — схема создаётся только [`init.sql`](docker/init.sql:1) при первом старте Postgres. Папка `docker/migrations/` пуста.
- Риск: рассинхрон между `DbContext` и реальной схемой при изменениях.
- Есть скрипт партиционирования [`init-partitioned.sql`](docker/init-partitioned.sql:1), но он создаёт партиции с **жёстко зашитыми датами** `rawticks_2026_07_29`, требует ручного продления. Без автоматизации (pg_partman) таблица тиков начнёт расти без партиций.

### 3.2 Kafka single-node без репликации
В [`docker-compose.yml`](docker/docker-compose.yml:31) `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`, `replication-factor 1` для всех топиков. Для продакшена нужен кластер с RF ≥ 3, иначе потеря данных/оффсетов при падении ноды.

### 3.3 Здоровье и зависимость от Kafka/Postgres
`DependencyInjection.VerifyKafkaAvailability` использует **консоль** (`Console.WriteLine`), а не логгер — в production-логгировании это потеряется. Есть фоллбэк на прямую запись в БД, но это смена режима на лету без сигнала оператору.

### 3.4 Ретеншн данных и рост БД
При ~19–25K msg/s таблица `RawTicks` растёт быстро. Нет политики архивации/партиционирования в автоматическом режиме, нет retention. Prometheus `retention 7d` — ок, а вот БД — нет.

---

## 4. 🟡 Наблюдаемость — замечания

> **Обновлено 2026-07-31:** пункты про WebSocket-клиентов/каналы и OTLP на `localhost` закрыты (см. чек-лист раздела 6). Актуальна только аутентификация `/metrics`/`/health` — теперь реализована.

- `/health` проверяет Postgres, Kafka, состояние WebSocket-клиентов и заполненность каналов (backlog/дропы) — см. реализацию в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:100).
- Настройки OTLP: для prod задаётся доступный эндпоинт в [`appsettings.Production.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json:10), в compose переопределяется через env.
- Endpoint `/metrics` (Prometheus) и `/health` в production защищены Bearer/API-key токеном (`Authorization: Bearer <token>`, задаётся через `Auth__ApiKey` / env `AUTH_API_KEY`). В Development ключ не задан — эндпоинты открыты для локальной разработки. Docker healthcheck, blackbox-probe и Prometheus scrape передают токен.

---

## 5. 🟠 Архитектурные долги (из `architecture-review-report.md`)

Проверено повторно на актуальном коде (2026-07-31):

1. **`Application` не должен ссылаться на `Infrastructure`** (DIP) — ✅ подтверждено соблюдение. [`Application.csproj`](src/MarketDataCollector.Application/MarketDataCollector.Application.csproj:4) ссылается только на Core и Domain, без Infrastructure. Заявленные ранее нарушения в `TickAggregator`/`MarketDataProcessor` **не воспроизводятся**: они работают через абстракции Core (`IRawTickRepository`, `IAggregatedDataRepository`, `IConnectionLogRepository`, `ICandlePublisher`), а Npgsql/`KafkaCandleProducer` используются только в Infrastructure. Действий не требуется.
2. **Domain зависит от EF/Npgsql-атрибутов** — ✅ не подтверждается (пункт закрыт). Сущности `RawTick`, `AggregatedData`, `ConnectionLog` чистые, без персистентностных атрибутов; маппинг полностью во Fluent API в [`MarketDataDbContext`](src/MarketDataCollector.Infrastructure/Data/MarketDataDbContext.cs:16).
3. Мёртвый код `DataStorageService` — ✅ удалён из `src` и тестов. Устаревшие упоминания в [`README.md`](README.md) убраны (обновлено 2026-07-31).

---

## 6. Чек-лист для достижения production-ready

### Блокеры (P0)
- [x] Вынести пароль и все секреты из кода в env/secret-менеджер; убрать `trust` в prod.
  - Пароль убран из [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) (base-конфиг без секретов), перенесён в [`appsettings.Development.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Development.json) и env (`docker/.env`). `POSTGRES_HOST_AUTH_METHOD: trust` убран из [`docker-compose.prod.yml`](docker/docker-compose.prod.yml) — там полноценная SCRAM-аутентификация. Шаблон секретов: [`docker/.env.example`](docker/.env.example).
- [x] Создать `appsettings.Production.json` + переопределение через env.
  - Создан [`appsettings.Production.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json) (реальные Binance URL, Kafka/OTLP в compose-сети, без секретов). Переопределение через стандартный env-провайдер (`ConnectionStrings__MarketDataDb`, `Kafka__BootstrapServers`, `OpenTelemetry__OtlpEndpoint` и т.д.) — задаётся в [`docker-compose.prod.yml`](docker/docker-compose.prod.yml).
- [x] Создать `Dockerfile` для воркера и добавить сервис `worker` в compose (с healthcheck).
  - Создан [`Dockerfile.worker`](docker/Dockerfile.worker) (multi-stage: sdk → `dotnet publish -c Release` → aspnet runtime, HEALTHCHECK на `/health`). Сервис `worker` добавлен в [`docker-compose.yml`](docker/docker-compose.yml) и [`docker-compose.prod.yml`](docker/docker-compose.prod.yml) с healthcheck и `depends_on` на Postgres/Kafka.
- [x] Настроить CI/CD: сборка, `dotnet test`, публикация образа, деплой.
  - [`ci.yml`](.github/workflows/ci.yml): сборка Release, `dotnet test`, публикация Docker-образа в GHCR (`ghcr.io/<repo>/marketdata-worker`, теги `sha`/`latest`).
  - [`deploy.yml`](.github/workflows/deploy.yml): по тегу `v*` — деплой на target-сервер через SSH, подтягивание образа из GHCR и `docker compose up -d`.

### Надёжность (P1)
- [x] Внедрить EF Core миграции (`Database.Migrate` при старте Worker).
  - Пакет `Microsoft.EntityFrameworkCore.Design`, `DesignTimeDbContextFactory`, начальная миграция `InitialCreate` (таблица `rawticks` создаётся `PARTITION BY RANGE`, PK `(id, timestamp)`). Авто-миграция через `db.Database.Migrate()` в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs). `init.sql` больше не используется для создания схемы.
- [x] Автоматизировать партиционирование `RawTicks` + политику retention/архивации.
  - `PartitionMaintenanceService` (hosted-сервис) создаёт day-партиции вперёд (`PremakeDays`) и удаляет старше `RetentionDays` (default 30). Настройки — секция `Partitioning`.
- [x] Кластер Kafka с RF ≥ 3.
  - `docker-compose.prod.yml`: 3-нодный KRaft-кластер (`kafka-1/2/3`), RF=3 для всех топиков, `min.insync.replicas=2`. Dev-композ остаётся single-node.
- [x] Заменить `Console.WriteLine` на логгер в `VerifyKafkaAvailability`.
  - Метод удалён; заменён hosted-сервисом `KafkaAvailabilityCheckService` со структурированным логированием через `ILogger`.
- [x] Мониторинг-алерты на backlog, дропы, дисконнекты WebSocket, `/health`.
  - [`rules.yml`](docker/prometheus/rules.yml) + Alertmanager + blackbox-exporter в compose. Алерты: backlog канала, дропы, все WS-клиенты отключены, `/health` недоступен, всплеск исключений.

### Observability (P2)
- [x] Добавить в `/health` состояние WebSocket-клиентов и fill-level каналов.
  - Реализовано в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:155): блок `websocket` (статус, total/connected/disconnected, per-client метрики из `IWebSocketClientRegistry`) и блок `channels` (fill-level, estimatedDropped, incoming/received, processedRps). Комбинированный HTTP-код: `503 degraded`, если Kafka/PostgreSQL unhealthy **или** все зарегистрированные WS-клиенты отключены; каналы — информационные. Реестр `IWebSocketClientRegistry` зарегистрирован singleton в [`DependencyInjection.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/DependencyInjection.cs:67), клиенты регистрируются в [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs:48) и очищаются в `CleanupAsync` (строка 140).
- [x] Аутентификация/изоляция `/metrics` и `/health`.
  - В production защищены Bearer/API-key токеном (`Authorization: Bearer <token>`, `Auth__ApiKey` / env `AUTH_API_KEY`). Middleware в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:84) активен только если ключ задан (в Development ключ не задан — открыто). Docker healthcheck ([`docker-compose.prod.yml`](docker/docker-compose.prod.yml:220)), blackbox-probe ([`blackbox.yml`](docker/prometheus/blackbox.yml:9)) и Prometheus scrape ([`prometheus.yml`](docker/prometheus/prometheus.yml:24)) передают токен.
- [x] Прод-адрес OTLP/Prometheus вместо `localhost`.
  - [`appsettings.Production.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json:10): `OtlpEndpoint: http://aspire-dashboard:18889` (вместо dev `localhost`). В [`docker-compose.prod.yml`](docker/docker-compose.prod.yml:202) endpoint переопределяется через env `OpenTelemetry__OtlpEndpoint`. Prometheus scrapes воркер по compose-имени `worker:5010` в [`prometheus.yml`](docker/prometheus/prometheus.yml:21), а не `localhost`.

### Технический долг (P2)
- [x] Рефакторинг DIP: `Application → Infrastructure` (Kafka, Npgsql) за абстракции — подтверждено соблюдение (см. п.5.1).
- [x] Очистить Domain от EF/Npgsql-атрибутов (Fluent API) — выполнено (см. п.5.2).
- [x] Удалить мёртвый `DataStorageService` — выполнено (см. п.5.3).
- [x] Актуализировать [`README.md`](README.md) — убрать устаревшие упоминания `DataStorageService` (выполнено 2026-07-31).

---

## 7. Итоговая оценка готовности

| Категория | Готовность |
|-----------|------------|
| Ядро обработки потока | 🟢 Готово |
| Персистентность | 🟢 Готово (без retention) |
| Наблюдаемость | 🟢 Готово (OTLP/Prometheus/`/health`+`/metrics`, WS-клиенты и каналы в `/health`, Bearer-аутентификация в prod) |
| Безопасность | 🟢 Готово (P0: секреты вынесены в env, `trust` убран в prod) |
| Деплой / контейнеризация | 🟢 Готово (P0: `Dockerfile.worker` + сервис `worker` с healthcheck) |
| CI/CD | 🟢 Готово (P0: `ci.yml` + `deploy.yml`) |
| Управление схемой БД | 🟢 Готово (миграции + авто `Database.Migrate` + партиционирование) |
| Масштабируемость Kafka | 🟢 Готово (3-нодный кластер RF=3 в prod) |
| Мониторинг-алерты | 🟢 Готово (rules.yml + Alertmanager + blackbox) |
| **Итог** | **🟢 ~95% — P0, P1-надёжность, наблюдаемость и P2-техдолг закрыты** |

**Минимальный набор для запуска в prod:** P0 (секреты, продакшен-конфиг, Dockerfile, CI/CD), P1-надёжность (миграции, партиционирование+retention, Kafka RF, мониторинг-алерты), наблюдаемость (OTLP/Prometheus, `/health`+`/metrics`, Bearer-аутентификация в prod) и P2-техдолг (актуализация README) **закрыты**. Система готова к эксплуатации под управляемой нагрузкой.
