# Развёртывание MarketDataCollector в production

Это инструкция по деплою. Она отвечает на вопрос «можно ли развернуть по одной кнопке?»:

- **Да, после однократной настройки** (раздел 1). Дальше каждый деплой — это одна кнопка «Run workflow» в GitHub Actions (или пуш тега `v*`).
- **Нет, сразу после клонирования** — сначала нужно один раз настроить секреты и сервер.

---

## 1. Однократная настройка (делается 1 раз)

### 1.1. Подготовить прод-сервер

Требования на сервере (Debian/Ubuntu, аналог для других ОС):
- Docker Engine + Docker Compose plugin (`docker compose version` должен работать).
- SSH-доступ (для GitHub Actions) и свободные порты.

Проверка:
```bash
docker --version
docker compose version
```

### 1.2. Создать GitHub Secrets

Откройте репозиторий → **Settings → Secrets and variables → Actions → New repository secret**.

| Secret | Назначение |
|--------|-----------|
| `DEPLOY_HOST` | IP/хостname прод-сервера |
| `DEPLOY_USER` | SSH-пользователь (например `ubuntu` или `deploy`) |
| `DEPLOY_SSH_KEY` | Приватный SSH-ключ (в формате `-----BEGIN OPENSSH PRIVATE KEY-----`) |
| `DEPLOY_PORT` | SSH-порт (обычно `22`) |
| `POSTGRES_DB` | Имя БД (например `MarketDataDb`) |
| `POSTGRES_USER` | Пользователь БД (например `marketdata_user`) |
| `POSTGRES_PASSWORD` | **Сильный пароль БД** |
| `EXCHANGE_WEBSOCKET_URL` | `wss://stream.binance.com:9443/ws/{symbol}@trade` (или другой источник) |
| `GHCR_USERNAME` | GitHub-username, под которым логинимся в GHCR на сервере |
| `GHCR_PAT` | Personal Access Token с правом **`read:packages`** (для `docker pull` на сервере) |

> **Важно про `GHCR_PAT`:** `GITHUB_TOKEN` работает только внутри job'ов, но не для `docker pull` с самого сервера. Для деплоя нужен личный PAT (`Settings → Developer settings → Personal access tokens → Generate new token (classic)`, scope `read:packages`).

### 1.3. Сделать GHCR-пакет публичным (рекомендуется)

Чтобы сервер мог пулить образ без лишней авторизации, либо:
- **Вариант A (публичный пакет):** Settings → Packages → ваш пакет → Make public. Тогда `GHCR_PAT` не обязателен.
- **Вариант B (приватный пакет):** оставить приватным и использовать `GHCR_PAT`/`GHCR_USERNAME` из п.1.2.

### 1.4. Создать локальный `.env` на сервере (если не хотите, чтобы его создавал пайплайн)

Пайплайн [`deploy.yml`](.github/workflows/deploy.yml) сам создаёт `/opt/marketdata-collector/docker/.env` из GitHub Secrets при каждом запуске. Поэтому вручную `.env` создавать не обязательно. Если хотите контролировать вручную — скопируйте [`docker/.env.example`](docker/.env.example) в `docker/.env` на сервере и заполните.

---

## 2. Запуск деплоя «по кнопке»

После настройки раздела 1 развёртывание занимает одну операцию:

### Способ 1 — кнопка в Actions (рекомендуется)
1. В GitHub откройте вкладку **Actions**.
2. Слева выберите workflow **Deploy**.
3. Нажмите **Run workflow** (внизу справа) → **Run workflow**.

Это соберёт Docker-образ, опубликует его в GHCR, скопирует compose-файлы на сервер, обновит `.env` и выполнит `docker compose up -d`.

### Способ 2 — пуш тега
```bash
git tag v1.0.0
git push origin v1.0.0
```

---

## 3. Что происходит внутри пайплайна

- **CI** ([`ci.yml`](.github/workflows/ci.yml)): на push в `main`/PR — `dotnet restore`, `dotnet build -c Release`, `dotnet test`, публикация образа в GHCR (`ghcr.io/<repo>/marketdata-worker`).
- **Deploy** ([`deploy.yml`](.github/workflows/deploy.yml)):
  1. `publish-image`: публикует образ в GHCR (теги `sha` + `latest`).
  2. `Copy deploy files`: копирует `docker-compose.prod.yml`, `prometheus.yml`, `rules.yml`, `alertmanager.yml`, `blackbox.yml` на сервер (`appleboy/scp-action`).
  3. `Deploy worker`: создаёт/обновляет `.env` из Secrets, логинится в GHCR, `docker compose pull && up -d`, ждёт healthcheck.

### 3.1. Миграции и партиционирование БД
- Схема БД создаётся **автоматически EF Core миграциями** при старте воркера (`Database.Migrate()` в [`Program.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs)). `init.sql` больше не используется — схемы из `docker/init.sql` и миграций **должны совпадать**.
- Таблица `rawticks` создаётся как **native-партиционированная** по `timestamp` (PK `(id, timestamp)`).
- Партиции автоматически обслуживает `PartitionMaintenanceService` (создание вперёд на `PremakeDays`, удаление старше `RetentionDays`). Настройки — секция `Partitioning` в [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json) / env `Partitioning__*`.
- Добавление новой миграции: `dotnet ef migrations add <Name> --project src/MarketDataCollector.Infrastructure/... --output-dir Data/Migrations`.

### 3.2. Kafka-кластер (RF=3)
- Production использует **3-нодный KRaft-кластер** (`kafka-1/2/3`) с `replication-factor 3` для всех топиков и `min.insync.replicas=2`.
- Worker подключается к `KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092`.
- При падении одной ноды кластер продолжает работать (quorum из 3 контроллеров).

### 3.3. Мониторинг и алерты
- Prometheus (`:9091`), Alertmanager (`:9093`), blackbox-exporter (`:9115`) разворачиваются в compose.
- Алерты в [`rules.yml`](docker/prometheus/rules.yml): backlog канала, дропы тиков, дисконнекты WebSocket, недоступность `/health`.
- Настройка получателей уведомлений — в [`alertmanager.yml`](docker/prometheus/alertmanager.yml) (по умолчанию без receivers, алерты видны в UI Alertmanager).

---

## 4. Проверка после деплоя

```bash
# Статус контейнеров
docker ps --filter name=marketdata-worker

# Health
curl -s http://localhost:5010/health | jq

# Логи воркера
docker logs --tail 50 marketdata-worker
```

`/health` должен вернуть `"status":"healthy"` и `checks.websocket.status = healthy`.

---

## 5. Переменные конфигурации (env-переопределение)

Приложение читает прод-конфиг из [`appsettings.Production.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.Production.json) и переопределяет его через env (`double-underscore` = вложенность):

| Переменная | Что задаёт |
|-----------|-----------|
| `ConnectionStrings__MarketDataDb` | Строка подключения к Postgres |
| `Kafka__BootstrapServers` | Адреса Kafka (для кластера — через запятую) |
| `Kafka__Enabled` | Включить/выключить Kafka |
| `OpenTelemetry__OtlpEndpoint` | OTLP-эндпоинт (Aspire/collector) |
| `ExchangeOptions__Exchanges__0__WebSocketUrl` | WebSocket-источник |
| `Partitioning__RetentionDays` | Срок хранения тиков (дней, default 30) |
| `Partitioning__PremakeDays` | На сколько дней вперёд создавать партиции |
| `Logging__LogLevel__Default` | Уровень логирования |

---

## 6. Известные ограничения (не P0)

- Пароль/секреты в коде убраны; в прод-конфиге и compose секретов нет — только `.env` (не коммитится) и GitHub Secrets.
- `init.sql` больше не используется для создания схемы — схему создают EF Core миграции. `docker/init.sql`/`init-partitioned.sql` оставлены как справка/историческая документация.
- Алерты не настроены на внешние каналы (e-mail/webhook) по умолчанию — заполните receivers в [`alertmanager.yml`](docker/prometheus/alertmanager.yml).
- Auto-migration (`Database.Migrate()` при старте) при нескольких репликах Worker может конкурировать — миграции идемпотентны, но для строгого контроля рассмотрите отдельный шаг деплоя.
