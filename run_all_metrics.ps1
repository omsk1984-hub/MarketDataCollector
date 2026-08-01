<#
.SYNOPSIS
    Запуск полного профилирования MarketDataCollector.Worker (counters + trace + gcdump).

.DESCRIPTION
    Обёртка над scripts/collect-all.ps1 с параметрами по умолчанию.
    Также поддерживает запуск через metrics.ps1 -Mode all.

.PARAMETER TraceProfile
    Профиль сбора dotnet-trace:
      gc-verbose     — аллокации/GC (по умолчанию)
      cpu-sampling   — CPU-стеки (topN)
      contention     — только contention-события (0x4000), без CPU-стеков
      contention-cpu — contention + CPU-sampling ОДНОВРЕМЕННО (для локализации lock contention
                       по стекам; требование Этапа 0 плана gen2-loh-and-lock-contention)

.EXAMPLE
    .\run_all_metrics.ps1
    .\run_all_metrics.ps1 -TraceProfile contention-cpu
#>

param(
    [ValidateSet("gc-verbose", "cpu-sampling", "contention", "contention-cpu")]
    [string]$TraceProfile = "gc-verbose"
)

# Запуск отдельного скрипта напрямую (быстрее, без промежуточного dispatcher)
& "$PSScriptRoot\scripts\collect-all.ps1" -TraceDuration 90 -GcDumpAtPeakSec 50 -TraceProfile $TraceProfile
