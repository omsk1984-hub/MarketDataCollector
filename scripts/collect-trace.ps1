<#
.SYNOPSIS
    Сбор allocation trace через dotnet-trace для MarketDataCollector.Worker.

.DESCRIPTION
    Запускает dotnet-trace collect с профилем gc-verbose для анализа аллокаций.
    Результат конвертируется в SpeedScope для визуализации.

.PARAMETER WorkerProcessName
    Имя процесса Worker. По умолчанию MarketDataCollector.Worker.

.PARAMETER TraceDuration
    Длительность сбора trace в секундах. По умолчанию 60.

.PARAMETER OutputDir
    Директория для результата. По умолчанию ./traces.

.EXAMPLE
    .\scripts\collect-trace.ps1

    .\scripts\collect-trace.ps1 -TraceDuration 90

    .\scripts\collect-trace.ps1 -WorkerProcessName "dotnet" -OutputDir "./my-traces"
#>

param(
    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [int]$TraceDuration = 60,
    [string]$OutputDir = "./traces"
)

$ErrorActionPreference = "Stop"

# Подключаем общие функции
. "$PSScriptRoot\common-functions.ps1"

# Убедиться, что dotnet tools установлены
Ensure-DotnetTools

Write-SectionHeader "Режим: dotnet-trace (allocation tracking)"

$traceFile = Join-Path (Resolve-Path $OutputDir) "allocation_trace_$(Get-Date -Format 'yyyyMMdd_HHmmss').nettrace"

# Найти PID
$workerPid = Find-ProcessId -ProcessName $WorkerProcessName

# Запустить trace
$traceJob = Start-TraceCollection -ProcessId $workerPid -DurationSec $TraceDuration -OutputFilePath $traceFile

# Ждать нужное время
Write-Host "[*] Сбор allocation trace в течение ${TraceDuration}с..." -ForegroundColor Cyan
$elapsed = 0
while ($elapsed -lt $TraceDuration) {
    if ($traceJob.Process.HasExited) {
        Write-Host "`n[!] dotnet-trace завершился раньше времени." -ForegroundColor Yellow
        break
    }
    Start-Sleep -Seconds 1
    $elapsed++
    if ($elapsed % 10 -eq 0) {
        Write-Host "    Прошло ${elapsed}s / ${TraceDuration}s..."
    }
}

# Остановить trace
Stop-TraceCollection -TraceProcess $traceJob.Process

# Проверить результат
if (Test-Path $traceFile) {
    $fileSize = (Get-Item $traceFile).Length
    $sizeStr = if ($fileSize -gt 1MB) { "$([math]::Round($fileSize/1MB, 2)) MB" } else { "$([math]::Round($fileSize/1KB, 1)) KB" }
    Write-Host "[+] Trace файл: $traceFile ($sizeStr)" -ForegroundColor Green

    # Конвертировать в SpeedScope
    Convert-TraceToSpeedScope -TraceFile $traceFile
} else {
    Write-Host "[!] Trace файл не создан." -ForegroundColor Red
}
