<#
.SYNOPSIS
    Диспетчер сбора метрик и/или профилирования MarketDataCollector.Worker.

.DESCRIPTION
    Делегирует вызов отдельным скриптам в папке scripts/:
    - counters → scripts/collect-counters.ps1
    - trace    → scripts/collect-trace.ps1
    - gcdump   → scripts/collect-gcdump.ps1
    - all      → scripts/collect-all.ps1

    Режимы:
    counters — сбор Prometheus-метрик в CSV
    trace    — dotnet-trace с allocation tracking (+ конвертация в SpeedScope)
    gcdump   — dotnet-gcdump (2 снапшота: на пике и после дренажа)
    all      — всё сразу: counters + trace + 2x gcdump

.PARAMETER Mode
    Режим работы. Допустимые значения: counters, trace, gcdump, all. По умолчанию counters.

.PARAMETER MetricsUrl
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.

.PARAMETER OutputDir
    Директория для результатов. По умолчанию ./traces.

.PARAMETER RefreshSeconds
    Интервал опроса метрик в секундах (counters/all). По умолчанию 5.

.PARAMETER WorkerProcessName
    Имя процесса Worker. По умолчанию MarketDataCollector.Worker.

.PARAMETER TraceDuration
    Длительность trace в секундах (trace/all). По умолчанию 60.

.PARAMETER GcDumpAtPeakSec
    Через сколько секунд взять первый gcdump (gcdump/all). По умолчанию 40.

.PARAMETER DrainWaitSec
    Сколько секунд ждать дренажа перед вторым gcdump (gcdump/all). По умолчанию 30.

.PARAMETER Duration
    Максимальная длительность сбора метрик в секундах (counters). 0 = без ограничений.

.EXAMPLE
    # Только сбор Prometheus-метрик
    .\metrics.ps1 -Mode counters

    # Только dotnet-trace (60 сек)
    .\metrics.ps1 -Mode trace -TraceDuration 60

    # Только gcdump: 2 снапшота
    .\metrics.ps1 -Mode gcdump -GcDumpAtPeakSec 40

    # Всё сразу
    .\metrics.ps1 -Mode all -TraceDuration 90 -GcDumpAtPeakSec 50

    # Запуск отдельного скрипта напрямую
    .\scripts\collect-counters.ps1 -Duration 120
    .\scripts\collect-trace.ps1 -TraceDuration 90
    .\scripts\collect-gcdump.ps1 -GcDumpAtPeakSec 50
    .\scripts\collect-all.ps1 -TraceDuration 90 -GcDumpAtPeakSec 50
#>

param(
    [ValidateSet("counters", "trace", "gcdump", "all")]
    [string]$Mode = "counters",

    [string]$MetricsUrl = "http://localhost:5010/metrics",
    [string]$OutputDir = "./traces",
    [int]$RefreshSeconds = 5,
    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [int]$TraceDuration = 60,
    [int]$GcDumpAtPeakSec = 40,
    [int]$DrainWaitSec = 30,

    # Устаревшие/совместимость
    [int]$Duration = 0,
    [string]$Url = ""
)

$ErrorActionPreference = "Stop"

# Если указан устаревший параметр Url — используем его как MetricsUrl
if ($Url -and -not $MetricsUrl) {
    $MetricsUrl = $Url
}

# ============================================================
# MAIN
# ============================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║      MarketDataCollector — Профилирование                ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Создать выходную директорию
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Делегируем вызов отдельным скриптам
$scriptsDir = Join-Path $PSScriptRoot "scripts"

switch ($Mode) {
    "counters" {
        & "$scriptsDir\collect-counters.ps1" -MetricsUrl $MetricsUrl `
            -OutputDir $OutputDir -RefreshSeconds $RefreshSeconds -Duration $Duration
    }
    "trace" {
        & "$scriptsDir\collect-trace.ps1" -WorkerProcessName $WorkerProcessName `
            -TraceDuration $TraceDuration -OutputDir $OutputDir
    }
    "gcdump" {
        & "$scriptsDir\collect-gcdump.ps1" -WorkerProcessName $WorkerProcessName `
            -GcDumpAtPeakSec $GcDumpAtPeakSec -DrainWaitSec $DrainWaitSec `
            -OutputDir $OutputDir -MetricsUrl $MetricsUrl
    }
    "all" {
        & "$scriptsDir\collect-all.ps1" -WorkerProcessName $WorkerProcessName `
            -MetricsUrl $MetricsUrl -OutputDir $OutputDir -RefreshSeconds $RefreshSeconds `
            -TraceDuration $TraceDuration -GcDumpAtPeakSec $GcDumpAtPeakSec `
            -DrainWaitSec $DrainWaitSec
    }
}

Write-Host ""
Write-Host "[√] Готово." -ForegroundColor Green
