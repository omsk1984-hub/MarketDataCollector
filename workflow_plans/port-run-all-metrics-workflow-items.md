# Список пунктов воркфлоу `port-run-all-metrics-to-csharp`

Сформирован в рамках воркфлоу `sequential-execution`.
Источник: `plans/port-run-all-metrics-to-csharp-plan.md`.
Статус пункта: `[ ]` не выполнен, `[x]` выполнен (реализован и подтверждён).

Пункты соответствуют компонентам плана. Для каждого: сначала проверка текущего состояния `tools/Profiler`, затем — если не соответствует DoD — план и реализация.

---

## Пункт 1. `Profiler.csproj` и `ProfilerOptions.cs`

- Описание/Проблема: проект `tools/Profiler/Profiler.csproj` автономный, не входит в `MarketDataCollector.sln`; подключены `Microsoft.Extensions.Hosting/Http/Logging/Logging.Console`; таргет .NET 8; кодировка UTF-8. `ProfilerOptions.cs` — record с параметрами по таблице из плана (TraceProfile, TraceDuration, GcDumpAtPeakSec, DrainWaitSec, WorkerProcessName, MetricsUrl, HealthUrl, HealthTimeoutSec, OutputDir, RefreshSeconds, HttpLogLevel).
- Целевой результат (DoD): csproj собирается без предупреждений, ссылки на пакеты корректны, не входит в sln; ProfilerOptions содержит все параметры с дефолтами из плана.
- Затрагиваемые файлы: `tools/Profiler/Profiler.csproj`, `tools/Profiler/Options/ProfilerOptions.cs`.
- Риски: лишние/отсутствующие пакеты, несоответствие дефолтов.

Статус: [ ]

---

## Пункт 2. `CommandLineParser.cs`

- Описание/Проблема: парсинг `--name value` и `--name=value`, регистронезависимо, camelCase/kebab-case; валидация TraceProfile; справка через `--help`/`-h`; при неизвестном аргументе — справка + `Environment.Exit(1)`.
- Целевой результат (DoD): парсер корректно обрабатывает все параметры ProfilerOptions, валидирует TraceProfile, выводит справку и корректно завершает с кодом 1 при ошибке.
- Затрагиваемые файлы: `tools/Profiler/Cli/CommandLineParser.cs`.
- Риски: неверный разбор значений, отсутствие валидации.

Статус: [ ]

---

## Пункт 3. `DiContainer.cs`

- Описание/Проблема: `ServiceCollection` + `ServiceProvider`, `AddLogging` (SimpleConsole, MinimumLevel Debug), `AddHttpClient` с клиентами `Metrics` и `Health` + `HttpLoggingHandler`, регистрация всех сервисов через интерфейсы (Singleton), `ProfilerOptions` как singleton, `IWaiters` разбит на `IPeakLoadWaiter`/`IDrainWaiter`.
- Целевой результат (DoD): все интерфейсы из `Core/Interfaces/` зарегистрированы на реализации, резолв `IProfilerOrchestrator` работает, регистрация стратегий `IProcessIdSource` в нужном порядке.
- Затрагиваемые файлы: `tools/Profiler/Cli/DiContainer.cs`.
- Риски: пропущенная регистрация, неверный порядок стратегий.

Статус: [ ]

---

## Пункт 4. `ConsoleUI.cs` и интерфейсы `IConsoleUI.cs`

- Описание/Проблема: цветной вывод через `Console.ForegroundColor`, `SectionHeader(title)` (рамка `=`), методы Info/Warn/Ok/Error/Detail, дублирование в `ILogger<ConsoleUI>` на соответствующих уровнях.
- Целевой результат (DoD): методы выводят с цветом и логируют через ILogger; конструктор принимает `ILogger<ConsoleUI>`.
- Затрагиваемые файлы: `tools/Profiler/Core/ConsoleUI.cs`, `tools/Profiler/Core/Interfaces/IConsoleUI.cs`.
- Риски: отсутствие логирования, нарушение цветовой схемы.

Статус: [ ]

---

## Пункт 5. `EnsureDotnetTools.cs`

- Описание/Проблема: `dotnet tool list --global`, поиск `dotnet-trace` и `dotnet-gcdump`, установка отсутствующих через `dotnet tool install --global`.
- Целевой результат (DoD): при отсутствии инструмента — устанавливает; при наличии — пропускает; логирует действия.
- Затрагиваемые файлы: `tools/Profiler/Core/EnsureDotnetTools.cs`.
- Риски: неправильные команды, отсутствие асинхронности.

Статус: [ ]

---

## Пункт 6. `ProcessIdSources/` и `ProcessFinder.cs`

- Описание/Проблема: три стратегии `IProcessIdSource` (TracePsSource — dotnet-trace ps; TargetProcessSource — Process.GetProcessesByName; WmiProcessSource — System.Management Win32_Process). `ProcessFinder` — координатор, перебирает стратегии по приоритету, при неудаче — понятное сообщение + Exit(1).
- Целевой результат (DoD): каждая стратегия реализована и корректно ищет PID; ProcessFinder перебирает в порядке регистрации и сообщает об ошибке.
- Затрагиваемые файлы: `tools/Profiler/Core/ProcessIdSources/*.cs`, `tools/Profiler/Core/ProcessFinder.cs`, `tools/Profiler/Core/Interfaces/IProcessIdSource.cs`, `IProcessFinder.cs`.
- Риски: WMI/System.Management на Windows, некорректный парсинг dotnet-trace ps.

Статус: [ ]

---

## Пункт 7. `ToolRunner.cs`

- Описание/Проблема: запуск внешних exe с `UseShellExecute=false`, `CreateNoWindow=true`, редирект stdout/stderr; асинхронное чтение через `ReadToEndAsync`; `WaitForExitAsync` с таймаутом; возврат объекта с Process/Task чтения/OutputPath.
- Целевой результат (DoD): нет deadlock'а при заполнении pipe-буфера, корректное ожидание и таймауты.
- Затрагиваемые файлы: `tools/Profiler/Core/ToolRunner.cs`, `tools/Profiler/Core/Interfaces/IToolRunner.cs`.
- Риски: синхронное чтение (deadlock), отсутствие таймаутов.

Статус: [ ]

---

## Пункт 8. `TraceCollector.cs` и `GcDumpCollector.cs`

- Описание/Проблема: TraceCollector — Start/Stop с профилями (`gc-verbose`, `cpu-sampling`, `contention`, `contention-cpu`), длительность в формате `hh:mm:ss`, фолбэк `taskkill`/`Kill`. GcDumpCollector — Collect с проверкой живости PID, ожиданием до 120с, фолбэком Kill, диагностикой ExitCode/размера файла.
- Целевой результат (DoD): корректные аргументы для каждого профиля, формат `hh:mm:ss`, надёжная остановка trace, диагностика.
- Затрагиваемые файлы: `tools/Profiler/Core/TraceCollector.cs`, `tools/Profiler/Core/GcDumpCollector.cs`, интерфейсы.
- Риски: неверный формат длительности, зависание при остановке.

Статус: [ ]

---

## Пункт 9. `SpeedScopeConverter.cs`

- Описание/Проблема: `dotnet-trace convert --format speedscope`, нормализация задвоенного суффикса через `ChangeExtension(".speedscope.json")`, диагностика наличия/размера файла, сообщение о битом trace.
- Целевой результат (DoD): конвертация создаёт `.speedscope.json`, нормализация имени работает, ошибки диагностируются.
- Затрагиваемые файлы: `tools/Profiler/Core/SpeedScopeConverter.cs`, `ISpeedScopeConverter.cs`.
- Риски: задвоенный суффикс, битый/нефинализированный trace.

Статус: [ ]

---

## Пункт 10. `Waiters/` (PeakLoadWaiter, DrainWaiter)

- Описание/Проблема: `IPeakLoadWaiter` — countdown с выводом каждые 5с; `IDrainWaiter` — опрос `/metrics` через `IHttpClientFactory.CreateClient("Metrics")`, поиск `processor_channel_backlog_count{...} <число>`, фолбэк на countdown, общий таймаут.
- Целевой результат (DoD): ISP соблюдён (два ролевых интерфейса), DrainWaiter корректно определяет завершение дренажа и фолбэк при недоступности /metrics.
- Затрагиваемые файлы: `tools/Profiler/Core/Waiters/*.cs`, `IPeakLoadWaiter.cs`, `IDrainWaiter.cs`.
- Риски: неверный парсинг метрики backlog, отсутствие фолбэка.

Статус: [ ]

---

## Пункт 11. `HealthCheckService.cs`

- Описание/Проблема: опрос `/health` каждые 2с через `IHttpClientFactory.CreateClient("Health")`; HTTP 200 + `status=="healthy"` → готово; 503/degraded → предупреждение; недоступность → повтор до HealthTimeoutSec; по истечении таймаута Exit(1).
- Целевой результат (DoD): корректная обработка healthy/degraded/недоступности и таймаута.
- Затрагиваемые файлы: `tools/Profiler/Core/HealthCheckService.cs`, `IHealthCheckService.cs`.
- Риски: неверная интерпретация статусов.

Статус: [ ]

---

## Пункт 12. `HttpLoggingHandler.cs`

- Описание/Проблема: `DelegatingHandler`, логирование запроса (метод, URL, время) и ответа (StatusCode, размер, категория); уровень из `ProfilerOptions.HttpLogLevel`; при `None` — отключение; тело не пишется.
- Целевой результат (DoD): все именованные клиенты логируют запросы через ILogger; уровень регулируется; тело не логируется.
- Затрагиваемые файлы: `tools/Profiler/Core/HttpLoggingHandler.cs`.
- Риски: засорение логов, неучтённый уровень None.

Статус: [ ]

---

## Пункт 13. `PrometheusParser.cs` и `Models.cs`

- Описание/Проблема: разбор `# HELP`, `# TYPE`, форматов `metric{labels} value` и `metric value`; labels через regex `key="value"` склеиваются `; `; `record MetricSample(...)`; обработка NaN/Inf/экспоненциальной записи.
- Целевой результат (DoD): парсер корректно обрабатывает все форматы и особые значения; MetricSample — record.
- Затрагиваемые файлы: `tools/Profiler/Core/PrometheusParser.cs`, `tools/Profiler/Core/Models.cs`, `IPrometheusParser.cs`.
- Риски: неверный regex, пропуск особых значений.

Статус: [ ]

---

## Пункт 14. `CountersCollector.cs`

- Описание/Проблема: фоновый сборщик через `IHttpClientFactory`, `IPrometheusParser`, `ILogger<CountersCollector>`; проверка доступности /metrics (до 6 попыток, пауза 3с); заголовок CSV `Timestamp,Metric,Labels,Type,Value,Description`; цикл каждые RefreshSeconds; UTF8 без BOM/с BOM (совместимость с analyze_counters.ps1); остановка через CancellationToken.
- Целевой результат (DoD): CSV пишется в UTF8, парсинг корректный, экранирование `"` в Description, фоновый цикл останавливается по токену.
- Затрагиваемые файлы: `tools/Profiler/Services/CountersCollector.cs`, `ICountersCollector.cs`.
- Риски: кодировка CSV, зависание фонового цикла.

Статус: [ ]

---

## Пункт 15. `ProfilerOrchestrator.cs`

- Описание/Проблема: точная последовательность `collect-all.ps1` (14 шагов): EnsureDotnetTools → HealthCheck → OutputDir/имена с таймстампом → FindProcessId → TraceCollector.Start → CountersCollector → WaitForPeakLoad → GcDump(PEAK) → ожидание trace → TraceCollector.Stop → WaitForDrain → GcDump(DRAINED) → пауза 2с → SpeedScope.Convert → стоп counters → ReportGenerator.Generate → сводка.
- Целевой результат (DoD): все 14 шагов реализованы в правильном порядке; зависимости через DI; создаются все артефакты.
- Затрагиваемые файлы: `tools/Profiler/Services/ProfilerOrchestrator.cs`, `IProfilerOrchestrator.cs`.
- Риски: нарушение порядка шагов, пропуск шага.

Статус: [ ]

---

## Пункт 16. `ReportGenerator.cs` и `Program.cs`

- Описание/Проблема: ReportGenerator генерирует `profiling_report_<ts>.md` (заголовок, таблица Configuration, таблица Output Files с размерами, предупреждения, Анализ, Next Steps). Program.cs — `async Task<int> Main`, UTF8, парсинг, DiContainer.Build, баннер, `AllAsync(cts.Token)`, Ctrl+C, dispose.
- Целевой результат (DoD): отчёт по шаблону; Program.cs корректно запускает оркестратор, обрабатывает Ctrl+C и освобождает ресурсы.
- Затрагиваемые файлы: `tools/Profiler/Reporting/ReportGenerator.cs`, `IReportGenerator.cs`, `tools/Profiler/Program.cs`.
- Риски: неполный отчёт, утечка ресурсов, отсутствие обработки Ctrl+C.

Статус: [ ]

---

## Пункт 17. Финальная проверка и критерии готовности

- Описание/Проблема: итоговая проверка по «Критериям готовности» плана: `dotnet build tools/Profiler` без предупреждений; `--help` корректна; DI работает; health-check отрабатывает; воспроизводится последовательность; SOLID соблюдён (SRP/OCP/ISP/DIP); `dotnet format tools/Profiler`.
- Целевой результат (DoD): сборка чистая, справка работает, SOLID-принципы соблюдены, формат применён.
- Затрагиваемые файлы: весь `tools/Profiler`.
- Риски: предупреждения сборки, нарушение SOLID.

Статус: [ ]

---

> Новые пункты, обнаруженные в ходе конвейера, добавляются в конец списка с тем же форматом.
