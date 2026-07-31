<#
.SYNOPSIS
    Запуск полного профилирования MarketDataCollector.Worker (counters + trace + gcdump).

.DESCRIPTION
    Обёртка над scripts/collect-all.ps1 с параметрами по умолчанию.
    Также поддерживает запуск через metrics.ps1 -Mode all.
#>

# Запуск отдельного скрипта напрямую (быстрее, без промежуточного dispatcher)
& "$PSScriptRoot\scripts\collect-all.ps1" -TraceDuration 90 -GcDumpAtPeakSec 50
