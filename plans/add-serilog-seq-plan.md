# План: Добавление Serilog + Seq

## Цель

Перевести структурированное логирование Worker'а с Microsoft.Extensions.Logging (вывод в консоль + OTLP в Aspire Dashboard) на **Serilog** с sink'ами **Console** и **Seq**. Seq становится основным хранилищем логов; OTLP-экспорт логов убирается. Метрики и трейсы OpenTelemetry остаются без изменений.

## Принятые архитектурные решения

1. **Serilog — провайдер `ILogger<T>`.** Все сервисы продолжают использовать `ILogger<T>` (см. `DependencyInjection.cs`), ничего в DI-регистрациях менять не нужно. Serilog подключается через `builder.Host.UseSerilog()` + `ReadFrom.Configuration`.
2. **Sink'и:** Console + Seq. Seq — основное хранилище, Console — для локальной разработки и `dotnet run`.
3. **OTLP-логи убираются.** Удаляется блок `builder.Logging.AddOpenTelemetry(...)` (строки 115–121 в `Program.cs`). Экспорт **метрик** (строка 108) и **трейсов** (строка 112) остаётся — они независимы.
4. **`crash.log` и глобальные обработчики исключений НЕ трогаются.** Это отдельный диагностический механизм для фатальных нативных крахов, не связанный с `ILogger`.
5. **Seq включается флагом конфигурации** `Seq:Enabled`. Если флаг выключен или `ServerUrl` недоступен — приложение продолжает работать на Console-синке (Seq-sink терпим к недоступности, буферизует/отбрасывает).
6. **Bootstrap-логгер** настраивается до `CreateBuilder`, чтобы поймать ошибки конфигурации/старта.

## Затрагиваемые файлы

| Файл | Изменение |
|------|-----------|
| `src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj` | Добавить NuGet-пакеты Serilog |
| `src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs` | Bootstrap-логгер, `UseSerilog`, убрать OTLP-логи |
| `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json` | Секция `Serilog` (база) |
| `.../appsettings.Development.json` | Seq URL для локальной разработки |
| `.../appsettings.LoadTest.json` | Seq URL для нагрузочного теста |
| `.../appsettings.Production.json` | Seq URL внутри docker-сети |
| `docker/docker-compose.yml` | Сервис `seq` + `Seq__ServerUrl` в worker |
| `docker/docker-compose.prod.yml` | Сервис `seq` (prod) |
| `docker/.env.example` | Переменная `SEQ_ENDPOINT` |

## Шаги реализации

### 1. NuGet-пакеты (csproj)

Добавить в `MarketDataCollector.Worker.csproj`:

```bash
dotnet add src/MarketDataCollector.Workers/MarketDataCollector.Worker/MarketDataCollector.Worker.csproj package Serilog
dotnet add ... package Serilog.Extensions.Hosting
dotnet add ... package Serilog.Settings.Configuration
dotnet add ... package Serilog.Sinks.Console
dotnet add ... package Serilog.Sinks.Seq
```

Итоговый `ItemGroup`:

```xml
<PackageReference Include="Serilog" Version="4.0.0" />
<PackageReference Include="Serilog.Extensions.Hosting" Version="8.0.0" />
<PackageReference Include="Serilog.Settings.Configuration" Version="8.0.0" />
<PackageReference Include="Serilog.Sinks.Console" Version="6.0.0" />
<PackageReference Include="Serilog.Sinks.Seq" Version="8.0.0" />
```

> Версии сверить при установке через `dotnet add package` — команда сама подберёт актуальные.

### 2. Program.cs

- Вверху (после `GCSettings`-блока, до `var builder = ...`) добавить:

```csharp
// ===== Serilog bootstrap (до CreateBuilder, чтобы поймать ошибки конфигурации) =====
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .CreateBootstrapLogger();
```

  *Требует вынести `var builder = WebApplication.CreateBuilder(args);` выше bootstrap-логгера* (он использует `builder.Configuration`), либо читать конфиг через `new ConfigurationBuilder()`. Предпочтителен вариант: создать `builder` первым, затем bootstrap-логгер, затем остальная настройка.

- Заменить строки 114–121 (блок OTLP-логов) — удалить `builder.Logging.AddOpenTelemetry(...)`. Метрики/трейсы (строки 101–112) оставить.

- После создания builder'а:

```csharp
builder.Host.UseSerilog((context, services, configuration) =>
    configuration
        .ReadFrom.Configuration(context.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext());
```

- В конце `Program.cs`, после `app.Run()`, добавить корректное закрытие Serilog:

```csharp
finally
{
    Log.CloseAndFlush();
}
```

  Оборачивается `app.Run()` в try/finally (при желании оставить `app.Run()` без try, тогда `Log.CloseAndFlush()` не вызывается явно, но это менее надёжно — рекомендуется try/finally).

- `using Serilog;` вверху файла.

### 3. Конфигурация appsettings

**appsettings.json (база)** — минимальная настройка провайдера и минимальные уровни:

```json
"Serilog": {
  "Using": [ "Serilog.Sinks.Console", "Serilog.Sinks.Seq" ],
  "MinimumLevel": {
    "Default": "Information",
    "Override": {
      "Microsoft.AspNetCore": "Warning",
      "Microsoft.EntityFrameworkCore.Database.Command": "Warning"
    }
  },
  "WriteTo": [
    { "Name": "Console" },
    {
      "Name": "Seq",
      "Args": {
        "serverUrl": "http://localhost:5341",
        "apiKey": ""
      }
    }
  ],
  "Enrich": [ "FromLogContext" ],
  "Properties": {
    "Application": "MarketDataCollector.Worker"
  }
},
"Seq": {
  "Enabled": true,
  "ServerUrl": "http://localhost:5341"
}
```

**appsettings.Development.json / LoadTest.json** — переопределение Seq URL (локально `localhost:5341`):

```json
"Seq": {
  "ServerUrl": "http://localhost:5341"
}
```

**appsettings.Production.json** — Seq внутри docker-сети:

```json
"Seq": {
  "ServerUrl": "http://seq:5341"
}
```

> Примечание по `Seq:Enabled`: если требуется полностью отключить Seq-синк без изменения appsettings, логику `WriteTo` можно сделать условной через `MinimumLevel`/вложенную конфигурацию. Упрощённо: в Production `Enabled=true`; для локальных профилей по умолчанию `Enabled=true` тоже, но Seq-sink терпим к недоступности. Флаг `Seq:Enabled` используется в Program.cs для пропуска добавления Seq-синка при необходимости.

### 4. docker-compose

**docker-compose.yml** — добавить сервис `seq`:

```yaml
seq:
  image: datalust/seq:latest
  container_name: marketdata-seq
  ports:
    - "5341:80"     # Seq Web UI + ingestion
    - "5342:5341"   # ingest API (опционально)
  environment:
    - ACCEPT_EULA=Y
  volumes:
    - seq_data:/data
  networks:
    - marketdata-network
  restart: unless-stopped
```

В `worker.environment` добавить:

```yaml
Seq__ServerUrl: ${SEQ_ENDPOINT:-http://seq:5341}
```

В `volumes:` добавить `seq_data:`.

**docker-compose.prod.yml** — аналогичный сервис `seq` (без хардкода, через `.env`), в `worker` — `Seq__ServerUrl: ${SEQ_ENDPOINT:?SEQ_ENDPOINT required in .env}`.

### 5. docker/.env.example

Добавить:

```
# Seq (адрес внутри compose-сети)
SEQ_ENDPOINT=http://seq:5341
```

### 6. Верификация

- `dotnet build MarketDataCollector.sln` — сборка без ошибок.
- `dotnet format MarketDataCollector.sln` — форматирование перед коммитом (правило проекта).
- Локальный запуск: убедиться, что логи уходят в консоль, а при доступном Seq (`docker compose up seq`) — появляются в Seq.
- Убедиться, что `/health` и `/metrics` продолжают работать (OTLP-метрики/трейсы не задеты).

## Что НЕ делаем

- Не трогаем `crash.log` и глобальные обработчики (`AppDomain.CurrentDomain.UnhandledException`, `TaskScheduler.UnobservedTaskException`).
- Не трогаем OTLP-экспорт **метрик** и **трейсов**.
- Не меняем DI-регистрации сервисов (они работают через `ILogger<T>`).
- Не подключаем Seq к тестовым/бенчмарк-проектам (затрагивается только Worker).

## Риски и митигирование

| Риск | Митигирование |
|------|---------------|
| Seq недоступен при старте → потеря логов | Seq-sink буферизует и не роняет приложение; Console-sink остаётся |
| Высокий RPS логирования → нагрузка на Seq | Seq-sink батчит по HTTP; уровни в hot-path уже ограничены; при необходимости можно ограничить `MinimumLevel` |
| Потеря логов в Aspire Dashboard | Принятое решение — Seq становится основным хранилищем; метрики/трейсы остаются в Aspire |
| Версии пакетов Serilog | Устанавливаются через `dotnet add package` (актуальные stable) |
