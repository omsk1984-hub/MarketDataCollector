<#
.SYNOPSIS
    Запуск полного профилирования MarketDataCollector.Worker через утилиту Profiler.

.DESCRIPTION
    Обёртка над готовым бинарником MarketDataCollector.Profiler (tools/Profiler).
    Имитирует вызов run_all_metrics.ps1 (collect-all.ps1), но прокидывает ВСЕ
    опции Profiler в CLI утилиты.

    Profiler выполняет всё сразу: counters + trace + 2x gcdump (режим all).

    Скрипт НЕ компилирует Profiler — предполагается, что он уже собран
    (tools/Profiler/bin/Debug/net8.0/MarketDataCollector.Profiler.exe).
    Для сборки используйте: .\tools\Profiler\compile.ps1

.PARAMETER TraceProfile
    Профиль сбора dotnet-trace:
      gc-verbose     — аллокации/GC (по умолчанию)
      cpu-sampling   — CPU-стеки (topN)
      contention     — только contention-события (0x4000), без CPU-стеков
      contention-cpu — contention + CPU-sampling ОДНОВРЕМЕННО (для локализации
                       lock contention по стекам)

.PARAMETER TraceDuration
    Длительность trace, секунд. По умолчанию 90.

.PARAMETER GcDumpAtPeakSec
    Момент первого gcdump (пик), секунд. По умолчанию 50.

.PARAMETER DrainWaitSec
    Ожидание дренажа перед вторым gcdump, секунд. По умолчанию 30.

.PARAMETER WorkerProcessName
    Имя процесса Worker. По умолчанию MarketDataCollector.Worker.

.PARAMETER MetricsUrl
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.

.PARAMETER HealthUrl
    Health-check endpoint Worker'а. По умолчанию http://localhost:5010/health.

.PARAMETER HealthTimeoutSec
    Таймаут ожидания healthy, секунд. По умолчанию 30.

.PARAMETER OutputDir
    Директория результатов. По умолчанию ./traces.

.PARAMETER RefreshSeconds
    Интервал опроса метрик, секунд. По умолчанию 5.

.PARAMETER HttpLogLevel
    Уровень логирования HTTP-запросов (Trace|Debug|Information|None).
    По умолчанию Debug.

.PARAMETER TopNEnabled
    Включить пост-анализ trace через dotnet-trace report topN (топ методов
    по времени на call stack). Результат сохраняется в *_topn.md. По умолчанию true.

.PARAMETER TopNCount
    Число методов в topN-отчёте. По умолчанию 15.

.PARAMETER TopNInclusive
    Считать topN по inclusive (с включением дочерних) времени. По умолчанию false.

.EXAMPLE
    .\run_all_profiler.ps1
    .\run_all_profiler.ps1 -TraceProfile contention-cpu
    .\run_all_profiler.ps1 -TraceDuration 120 -GcDumpAtPeakSec 70 -OutputDir .\traces\prof
#>

[CmdletBinding()]
param(
    [ValidateSet("gc-verbose", "cpu-sampling", "contention", "contention-cpu")]
    [string]$TraceProfile = "gc-verbose",

    [int]$TraceDuration = 90,
    [int]$GcDumpAtPeakSec = 50,
    [int]$DrainWaitSec = 30,
    [int]$DrainConsecutiveZeroChecks = 3,
    [int]$DrainPollIntervalSec = 2,
    [int]$DrainSignalTimeoutSec = 180,

    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [string]$MetricsUrl = "http://localhost:5010/metrics",
    [string]$HealthUrl = "http://localhost:5010/health",
    [int]$HealthTimeoutSec = 30,

    [string]$OutputDir = "./traces",
    [int]$RefreshSeconds = 5,

    [ValidateSet("Trace", "Debug", "Information", "None")]
    [string]$HttpLogLevel = "Debug",

    [bool]$TopNEnabled = $true,
    [int]$TopNCount = 15,
    [bool]$TopNInclusive = $false
)

$ErrorActionPreference = "Stop"

# ============================================================
# Определяем путь к собранному бинарнику Profiler
# ============================================================
$exe = Join-Path $PSScriptRoot "tools\Profiler\bin\Debug\net8.0\MarketDataCollector.Profiler.exe"

if (-not (Test-Path $exe)) {
    Write-Host "Не найден собранный Profiler: $exe" -ForegroundColor Red
    Write-Host "Сначала соберите утилиту:" -ForegroundColor Yellow
    Write-Host "  .\tools\Profiler\compile.ps1" -ForegroundColor Cyan
    exit 1
}

# ============================================================
# Вывод конфигурации запуска
# ============================================================
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   MarketDataCollector — Профилирование (Profiler)        ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Executable:                 $exe"
Write-Host "TraceProfile:               $TraceProfile"
Write-Host "TraceDuration:              $TraceDuration сек"
Write-Host "GcDumpAtPeakSec:            $GcDumpAtPeakSec сек"
Write-Host "DrainWaitSec:               $DrainWaitSec сек"
Write-Host "DrainConsecutiveZeroChecks: $DrainConsecutiveZeroChecks"
Write-Host "DrainPollIntervalSec:       $DrainPollIntervalSec сек"
Write-Host "DrainSignalTimeoutSec:       $DrainSignalTimeoutSec сек"
Write-Host "WorkerProcess:              $WorkerProcessName"
Write-Host "MetricsUrl:                 $MetricsUrl"
Write-Host "HealthUrl:                  $HealthUrl"
Write-Host "OutputDir:                  $OutputDir"
Write-Host "HttpLogLevel:               $HttpLogLevel"
Write-Host "TopNEnabled:                $TopNEnabled"
Write-Host "TopNCount:                  $TopNCount"
Write-Host "TopNInclusive:              $TopNInclusive"
Write-Host ""

# ============================================================
# Формируем аргументы Profiler
# ============================================================
$profilerArgs = @(
    "--trace-profile", "$TraceProfile"
    "--trace-duration", "$TraceDuration"
    "--gc-dump-at-peak-sec", "$GcDumpAtPeakSec"
    "--drain-wait-sec", "$DrainWaitSec"
    "--drain-consecutive-zero-checks", "$DrainConsecutiveZeroChecks"
    "--drain-poll-interval-sec", "$DrainPollIntervalSec"
    "--drain-signal-timeout-sec", "$DrainSignalTimeoutSec"
    "--worker-process-name", "$WorkerProcessName"
    "--metrics-url", "$MetricsUrl"
    "--health-url", "$HealthUrl"
    "--health-timeout-sec", "$HealthTimeoutSec"
    "--output-dir", "$OutputDir"
    "--refresh-seconds", "$RefreshSeconds"
    "--http-log-level", "$HttpLogLevel"
    "--topn-enabled", "$TopNEnabled"
    "--topn-count", "$TopNCount"
    "--topn-inclusive", "$TopNInclusive"
)

# ============================================================
# Запуск. Используем splatting (& $exe @profilerArgs) вместо
# ручного кавычения + cmd /c.
#
# ПРЕЖДЕ: $quoted = (... | % { "`"$_`"" }) -join ' '; cmd /c $command
# ломало пары "--name value" из-за особых правил cmd /c по отбрасыванию
# кавычек. В результате Profiler.exe получал дефолтный gc-verbose вместо
# переданного профиля (напр. contention-cpu) — contention-события не
# собирались. Splatting передаёт каждый аргумент отдельно с корректным
# экранированием командной строки Windows.
# ============================================================
& $exe @profilerArgs
exit $LASTEXITCODE
