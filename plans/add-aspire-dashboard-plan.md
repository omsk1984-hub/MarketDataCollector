# План: Подключение Aspire Dashboard для сбора OTLP-данных

## Цель
Добавить Aspire Dashboard в Docker Compose и перенаправить OTLP-экспортер OpenTelemetry на Aspire Dashboard для визуализации метрик, трейсинга и логов.

---

## Мотивация выбора Aspire Dashboard

| Критерий | Aspire Dashboard |
|----------|:----------------:|
| Сложность запуска | ⭐⭐⭐⭐⭐ (1 контейнер) |
| Traces | ✅ |
| Metrics | ✅ |
| Logs | ✅ |
| OTLP native | ✅ (gRPC на порт 18889) |
| Веб-UI | ✅ (http://localhost:19000) |
| Нагрузка на систему | Минимальная |
| Ресурсы | ~200MB RAM |

---

## Архитектура до/после

### До (без внешнего сбора OTLP)
```
MarketDataCollector.Worker
  └── OpenTelemetry SDK
       └── OTLP Exporter → http://localhost:4317 (никуда не шлёт)
```

### После (с Aspire Dashboard)
```
MarketDataCollector.Worker (хостовый процесс)
  └── OpenTelemetry SDK
       └── OTLP Exporter → http://localhost:18889 (gRPC)
                               │
                    ┌──────────▼──────────┐
                    │  Aspire Dashboard   │
                    │  Docker контейнер    │
                    │  Порты:             │
                    │  - 18889 (OTLP)     │
                    │  - 19000 (Web UI)   │
                    └─────────────────────┘
```

---

## Шаг 1: Добавить Aspire Dashboard в docker-compose.yml

**Файл:** [`docker/docker-compose.yml`](docker/docker-compose.yml)

Добавить новый сервис:

```yaml
  aspire-dashboard:
    image: mcr.microsoft.com/dotnet/aspire-dashboard:9.1
    container_name: marketdata-aspire-dashboard
    ports:
      - "18889:18889"  # OTLP gRPC endpoint (принимает данные от приложений)
      - "19000:18888"  # Dashboard Web UI (18888 внутренний → 19000 внешний)
    environment:
      - DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS=true
    networks:
      - marketdata-network
    restart: unless-stopped
```

**Что делает:**
- `mcr.microsoft.com/dotnet/aspire-dashboard:9.1` — официальный стабильный образ
- Порт `18889` — принимает OTLP gRPC (traces + metrics + logs)
- Порт `19000` — веб-интерфейс дашборда
- `DOTNET_DASHBOARD_UNSECURED_ALLOW_ANONYMOUS=true` — отключает аутентификацию для dev-окружения
- Подключается к существующей сети `marketdata-network`

---

## Шаг 2: Обновить OTLP endpoint в appsettings.json

**Файл:** [`src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json)

Изменить значение `OtlpEndpoint`:

```json
"OpenTelemetry": {
  "OtlpEndpoint": "http://localhost:18889",
  "ServiceName": "MarketDataCollector.Worker"
}
```

**Почему порт 18889, а не 4317:**
- Aspire Dashboard использует порт `18889` для OTLP gRPC (стандартный порт .NET Aspire)
- Worker запускается на хосте (не в Docker) и обращается к `localhost:18889`, который пробрасывается в контейнер

---

## Шаг 3: Обновить README.md

**Файл:** [`README.md`](README.md)

Добавить в секцию "Быстрый старт → Шаг 1" информацию о Aspire Dashboard.

**Где добавить:**

После таблицы сервисов (строка ~284) добавить строку:

```markdown
| **Aspire Dashboard** | OpenTelemetry Dashboard (метрики, трейсинг, логи) | `http://localhost:19000` |
```

И в конце шага 1 добавить подсказку:

```markdown
### После запуска

Откройте Aspire Dashboard в браузере: [http://localhost:19000](http://localhost:19000)

Вы увидите:
- **Traces** — трейсинг запросов EF Core и WebSocket-соединений
- **Metrics** — метрики .NET Runtime (GC, CPU, память) и кастомные метрики
- **Logs** — структурированные логи приложения
- **Structured** — просмотр всех сигналов в едином интерфейсе
```

---

## Шаг 4: Проверка работоспособности

### 4.1. Перезапустить Docker окружение

```powershell
cd docker
docker-compose down
docker-compose up -d
```

### 4.2. Проверить, что Aspire Dashboard запущен

```powershell
docker logs marketdata-aspire-dashboard
```

Ожидаемый вывод: логи запуска с информацией о портах.

### 4.3. Проверить веб-интерфейс

Открыть в браузере: http://localhost:19000

### 4.4. Запустить Worker

```powershell
dotnet run --project src/MarketDataCollector.Workers/MarketDataCollector.Worker
```

### 4.5. Проверить поступление данных в Dashboard

1. Открыть http://localhost:19000
2. Перейти на вкладку **Traces** — должны появиться spans от EF Core
3. Перейти на вкладку **Metrics** — должны появиться метрики .NET Runtime
4. Перейти на вкладку **Logs** — должны появиться логи приложения

---

## Возможные проблемы и решения

| Проблема | Причина | Решение |
|----------|---------|---------|
| Worker не стартует с ошибкой подключения к OTLP | Aspire Dashboard не запущен | Убедиться, что `docker-compose up -d` выполнен |
| Нет данных в Dashboard | Неправильный порт | Проверить, что в appsettings.json порт `18889` |
| Dashboard UI не открывается | Порт 19000 занят | Проверить `docker ps`, сменить host-порт при необходимости |
| OTLP connection refused | Worker стартует раньше Dashboard | Просто подождать и перезапустить Worker |

---

## Итоговая схема взаимодействия

```
┌─────────────────────────────────────────────────────────────┐
│                    Хост (Windows)                            │
│                                                              │
│  MarketDataCollector.Worker (.NET 8)                         │
│    ├── OpenTelemetry SDK                                     │
│    │   ├── Metrics → OTLP Exporter ─────────────┐            │
│    │   ├── Traces  → OTLP Exporter ─────────────┤            │
│    │   └── Logs    → OTLP Exporter ─────────────┤            │
│    └── PostgreSQL (напрямую, порт 5433)          │            │
│                                                  │            │
│  Docker (docker-compose)                         │            │
│    ├── postgres:5433                             │            │
│    ├── kafka:9094                                │            │
│    ├── kafdrop:9000                              │            │
│    └── aspire-dashboard                          │            │
│        ├── port 18889 ◄──────────────────────────┘            │
│        └── port 19000 (Web UI)                               │
└─────────────────────────────────────────────────────────────┘
```
