# План: встроенный health-check и метрики у самого профайлера

## Проблема

Сейчас `MarketDataCollector.Profiler` — короткоживущая консольная утилита без HTTP-сервера.
Она только *клиент*: опрашивает `/health` и `/metrics` Worker'а (`http://localhost:5010`),
но сама ничего не слушает и собственных метрик не отдаёт.

Требуется: пока профайлер работает, поднять внутри него HTTP-сервер (Kestrel), который
отдаёт собственный `/health` с JSON-статусом и **некоторыми live-метриками** для ручного
просмотра (браузер/curl). Отдельный `/metrics` в Prometheus-формате **не нужен** — метрики
используются только для ручного просмотра.

Сервер поднимается в `Program.cs` и останавливается в конце работы.

## Решения

1. **HTTP-сервер через Kestrel.** Добавляем в `Profiler.csproj`:
   ```xml
   <FrameworkReference Include="Microsoft.AspNetCore.App" />
   ```
   (даёт Kestrel/WebApplication без лишних пакетов; `prometheus-net` **не нужен**, т.к. отдельного
   `/metrics` не делаем). Используем `WebApplicationBuilder` + `app.MapGet("/health", ...)`.

2. **Реестр метрик** — `IProfilerMetricsRegistry` (singleton), накапливает счётчики цикла
   профилирования (обновляются оркестратором по ходу шагов):
   - `status` — `healthy` / `degraded` (сервер жив или завершается).
   - `currentStep` — текущий шаг профилирования (строка).
   - `traceDurationSeconds` — фактическая длительность trace.
   - `gcdumpPeakSuccess` / `gcdumpDrainedSuccess` — флаги успешности (bool).
   - `countersSamples` — число сэмплов счётчиков Worker'а.
   - `speedScopeSuccess` — флаг успешности конвертации (bool).
   - `elapsedSeconds` — общее время работы профайлера.

3. **`/health` профайлера** отдаёт JSON:
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
   HTTP-код 200 пока оркестратор работает; при завершении/ошибке — 503 и `status: "degraded"`.

## Архитектура (интерфейс в `Core`, реализация в `Services`, singleton в DI)

- `tools/Profiler/Core/Interfaces/IProfilerMetricsRegistry.cs` — интерфейс реестра метрик.
- `tools/Profiler/Services/ProfilerMetricsRegistry.cs` — реализация (потокобезопасная, с `lock`).
- `tools/Profiler/Core/Interfaces/IProfilerHttpServer.cs` — интерфейс запуска сервера.
- `tools/Profiler/Services/ProfilerHttpServer.cs` — реализация: поднимает Kestrel, маппит `/health`.
- `tools/Profiler/Options/ProfilerOptions.cs` — новые опции:
  - `HttpPort` (int, default `5100` — отдельный порт, не конфликтует с Worker'ом `5010`),
  - `HttpEnabled` (bool, default `true`).
- `tools/Profiler/Cli/CommandLineParser.cs` — новые аргументы: `--http-port`, `--http-enabled`.
- `tools/Profiler/Cli/DiContainer.cs` — регистрация `IProfilerMetricsRegistry`, `IProfilerHttpServer`.
- `tools/Profiler/Program.cs` — поднятие сервера перед оркестратором, остановка в `finally`.
- `tools/Profiler/Services/ProfilerOrchestrator.cs` — обновление метрик по ходу шагов.

## Шаги реализации

1. Обновить `tools/Profiler/Profiler.csproj`: добавить `FrameworkReference Microsoft.AspNetCore.App`.
2. Создать `IProfilerMetricsRegistry` + `ProfilerMetricsRegistry`.
3. Создать `IProfilerHttpServer` + `ProfilerHttpServer` (Kestrel, только `/health`).
4. Расширить `ProfilerOptions` опциями `HttpPort` и `HttpEnabled`.
5. Добавить парсинг аргументов `--http-port`, `--http-enabled` в `CommandLineParser` + help-текст.
6. Зарегистрировать новые сервисы в `DiContainer`.
7. Поднять/остановить сервер в `Program.cs` (try/finally, включая Ctrl+C).
8. Обновлять метрики в `ProfilerOrchestrator` по ходу шагов.
9. Собрать и проверить: запустить профайлер, открыть `http://localhost:5100/health` в браузере/curl.
10. Обновить `tools/Profiler/README.md`.

## Тесты

- `curl http://localhost:5100/health` → 200 + JSON `{"status":"healthy",...,"metrics":{...}}`.
- Во время работы метрики меняются (countersSamples растёт, currentStep обновляется).
- При завершении профилирования сервер останавливается корректно; при Ctrl+C тоже.
