<#
.SYNOPSIS
    Сбор dotnet-gcdump (2 снапшота) для MarketDataCollector.Worker.

.DESCRIPTION
    Делает 2 снапшота управляемой кучи:
    - На пике нагрузки (через GcDumpAtPeakSec секунд)
    - После дренажа канала (через DrainWaitSec секунд)

    Результаты открываются в Visual Studio (Debug → Memory Usage → Load Snapshot).

.PARAMETER WorkerProcessName
    Имя процесса Worker. По умолчанию MarketDataCollector.Worker.

.PARAMETER GcDumpAtPeakSec
    Через сколько секунд взять первый снапшот (на пике). По умолчанию 40.

.PARAMETER DrainWaitSec
    Сколько секунд ждать дренажа перед вторым снапшотом. По умолчанию 30.

.PARAMETER OutputDir
    Директория для результатов. По умолчанию ./traces.

.PARAMETER MetricsUrl
    URL /metrics для проверки backlog при дренаже. По умолчанию http://localhost:5010/metrics.

.EXAMPLE
    .\scripts\collect-gcdump.ps1

    .\scripts\collect-gcdump.ps1 -GcDumpAtPeakSec 50 -DrainWaitSec 20

    .\scripts\collect-gcdump.ps1 -WorkerProcessName "dotnet"
#>

param(
    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [int]$GcDumpAtPeakSec = 40,
    [int]$DrainWaitSec = 30,
    [string]$OutputDir = "./traces",
    [string]$MetricsUrl = "http://localhost:5010/metrics"
)

$ErrorActionPreference = "Stop"

# Подключаем общие функции
. "$PSScriptRoot\common-functions.ps1"

# Убедиться, что dotnet tools установлены
Ensure-DotnetTools

Write-SectionHeader "Режим: dotnet-gcdump (2 снапшота)"

$workerPid = Find-ProcessId -ProcessName $WorkerProcessName

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$peakFile = Join-Path (Resolve-Path $OutputDir) "snapshot_peak_$timestamp.gcdump"
$drainedFile = Join-Path (Resolve-Path $OutputDir) "snapshot_drained_$timestamp.gcdump"

# Снапшот #1: на пике нагрузки
WaitFor-PeakLoad -Seconds $GcDumpAtPeakSec
Collect-GcDump -ProcessId $workerPid -OutputPath $peakFile -Label "PEAK"

# Ожидание дренажа
WaitFor-Drain -TimeoutSec $DrainWaitSec -MetricsEndpoint $MetricsUrl

# Снапшот #2: после стабилизации
Collect-GcDump -ProcessId $workerPid -OutputPath $drainedFile -Label "DRAINED"

Write-Host ""
Write-Host "[+] Результаты gcdump:" -ForegroundColor Green
Write-Host "    Peak:    $peakFile" -ForegroundColor DarkGray
Write-Host "    Drained: $drainedFile" -ForegroundColor DarkGray
Write-Host ""
Write-Host "    Для анализа откройте файлы в Visual Studio:" -ForegroundColor DarkGray
Write-Host "    Debug → Memory Usage → Load Snapshot" -ForegroundColor DarkGray
