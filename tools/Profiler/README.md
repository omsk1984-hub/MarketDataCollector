# Profiler

Standalone .NET 8 console-утилита профилирования `MarketDataCollector.Worker`.
Портирует цепочку PowerShell-скриптов (`run_all_metrics.ps1` → `collect-all.ps1` →
`common-functions.ps1`) в чистый C#.

Проект **не входит** в `MarketDataCollector.sln` и собирается отдельно.

## Возможности

- Health-check перед началом профилирования (endpoint `/health`).
- Сбор `allocation_trace_*.nettrace` через `dotnet-trace` (профили:
  `gc-verbose`, `cpu-sampling`, `contention`, `contention-cpu`).
- Фоновый сбор Prometheus-метрик `/metrics` в `counters_*.csv` (UTF-8).
- Два дампа кучи: `snapshot_peak_*.gcdump` (пик) и `snapshot_drained_*.gcdump`
  (после дренажа очередей).
- Конвертация trace в SpeedScope-формат.
- Markdown-отчёт `profiling_report_*.md`.
- Встроенный HTTP-сервер профайлера (`http://localhost:5100/health`) с собственным
  JSON-статусом и live-метриками для ручного просмотра.

## Требования

- .NET 8 SDK.
- Windows (используется `System.Management`/WMI и `taskkill`).
- Глобальные инструменты `dotnet-trace` и `dotnet-gcdump` — при необходимости
  утилита доустанавливает их автоматически.

## Сборка и запуск

```bash
dotnet build tools/Profiler/Profiler.csproj
```

Запуск с параметрами по умолчанию:

```bash
dotnet run --project tools/Profiler
```

Перед запуском обязательно должна быть запущена нагрузка (например,
`tests/FakeTickServer` + `MarketDataCollector.Worker`).

## Параметры

| Аргумент | Описание | По умолчанию |
|----------|----------|--------------|
| `--trace-profile` | Профиль trace | `gc-verbose` |
| `--trace-duration` | Длительность trace (сек) | `90` |
| `--gc-dump-at-peak-sec` | Момент первого gcdump (сек) | `50` |
| `--drain-wait-sec` | Ожидание дренажа (сек) | `30` |
| `--worker-process-name` | Имя процесса Worker | `MarketDataCollector.Worker` |
| `--metrics-url` | Prometheus endpoint | `http://localhost:5010/metrics` |
| `--health-url` | Health endpoint | `http://localhost:5010/health` |
| `--health-timeout-sec` | Таймаут health-check | `30` |
| `--output-dir` | Директория результатов | `./traces` |
| `--refresh-seconds` | Интервал опроса метрик | `5` |
| `--http-log-level` | Уровень HTTP-логирования | `Debug` |
| `--http-port` | Порт встроенного health-сервера профайлера | `5100` |
| `--http-enabled` | Включить встроенный health-сервер профайлера | `true` |
| `--help`, `-h` | Справка | — |

Поддерживаются форматы `--name value` и `--name=value`; имена регистронезависимы
(работают `camelCase` и `kebab-case`).

Пример:

```bash
dotnet run --project tools/Profiler -- --trace-profile contention-cpu --trace-duration 120 --output-dir ./traces
```

## Встроенный health-сервер

Во время работы профайлер поднимает собственный HTTP-сервер (Kestrel) на
`http://localhost:5100/health` (порт настраивается через `--http-port`, отключается
через `--http-enabled=false`). Сервер отдаёт JSON со статусом и live-метриками цикла
профилирования для ручного просмотра (браузер/curl):

```bash
curl http://localhost:5100/health
```

```json
{
  "status": "healthy",
  "currentStep": "6. Сбор счётчиков",
  "metrics": {
    "traceDurationSeconds": 90.0,
    "gcdumpPeakSuccess": true,
    "gcdumpDrainedSuccess": true,
    "countersSamples": 12,
    "speedScopeSuccess": true,
    "elapsedSeconds": 135.4
  },
  "timestamp": "2026-08-01T..."
}
```

HTTP-код 200, пока оркестратор работает; при завершении/ошибке — 503 и `status: "degraded"`.
Сервер останавливается в `finally`, в том числе при Ctrl+C.

## Структура проекта

```
tools/Profiler/
├── Profiler.csproj
├── Program.cs
├── Options/            ProfilerOptions (record)
├── Cli/                CommandLineParser, DiContainer
├── Core/
│   ├── Interfaces/     контракты (SOLID), в т.ч. IProfilerMetricsRegistry, IProfilerHttpServer
│   ├── Models.cs       DTO (record)
│   ├── ProfilerMetricsSnapshot.cs, ProfilerHealthResponse.cs   DTO встроенного /health
│   ├── ConsoleUI.cs
│   ├── EnsureDotnetTools.cs
│   ├── ProcessFinder.cs + ProcessIdSources/{TracePs,TargetProcess,Wmi}Source.cs
│   ├── ToolRunner.cs
│   ├── TraceCollector.cs
│   ├── GcDumpCollector.cs
│   ├── SpeedScopeConverter.cs
│   ├── Waiters/{PeakLoadWaiter,DrainWaiter}.cs
│   ├── HttpLoggingHandler.cs
│   ├── HealthCheckService.cs
│   └── PrometheusParser.cs
├── Services/           CountersCollector, ProfilerOrchestrator, ProfilerMetricsRegistry, ProfilerHttpServer
└── Reporting/          ReportGenerator
```
