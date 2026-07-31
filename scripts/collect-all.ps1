<#
.SYNOPSIS
    Полное профилирование MarketDataCollector.Worker: counters + trace + gcdump.

.DESCRIPTION
    Запускает все три типа сбора одновременно:
    1. Prometheus-метрики (counters) в CSV — в фоновом Job
    2. dotnet-trace (gc-verbose) — в фоне
    3. dotnet-gcdump — 2 снапшота (на пике и после дренажа)

    Генерирует сводный Markdown-отчёт.

.PARAMETER WorkerProcessName
    Имя процесса Worker. По умолчанию MarketDataCollector.Worker.

.PARAMETER MetricsUrl
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.

.PARAMETER OutputDir
    Директория для результатов. По умолчанию ./traces.

.PARAMETER RefreshSeconds
    Интервал опроса метрик в секундах. По умолчанию 5.

.PARAMETER TraceDuration
    Длительность trace в секундах. По умолчанию 60.

.PARAMETER GcDumpAtPeakSec
    Через сколько секунд взять первый gcdump (на пике). По умолчанию 40.

.PARAMETER DrainWaitSec
    Сколько секунд ждать дренажа перед вторым gcdump. По умолчанию 30.

.EXAMPLE
    .\scripts\collect-all.ps1

    .\scripts\collect-all.ps1 -TraceDuration 90 -GcDumpAtPeakSec 50

    .\scripts\collect-all.ps1 -WorkerProcessName "dotnet" -OutputDir "./my-traces"
#>

param(
    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [string]$MetricsUrl = "http://localhost:5010/metrics",
    [string]$OutputDir = "./traces",
    [int]$RefreshSeconds = 5,
    [int]$TraceDuration = 60,
    [int]$GcDumpAtPeakSec = 40,
    [int]$DrainWaitSec = 30
)

$ErrorActionPreference = "Stop"

# Подключаем общие функции
. "$PSScriptRoot\common-functions.ps1"

# Убедиться, что dotnet tools установлены
Ensure-DotnetTools

Write-SectionHeader "Режим: ALL (counters + trace + gcdump)"

# ============================================================
# 0. Подготовка
# ============================================================
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}
$resolveDir = (Resolve-Path $OutputDir).Path
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'

$traceFile = Join-Path $resolveDir "allocation_trace_$timestamp.nettrace"
$peakFile  = Join-Path $resolveDir "snapshot_peak_$timestamp.gcdump"
$drainedFile = Join-Path $resolveDir "snapshot_drained_$timestamp.gcdump"
$csvFile   = Join-Path $resolveDir "counters_$timestamp.csv"
$reportFile = Join-Path $resolveDir "profiling_report_$timestamp.md"

$workerPid = Find-ProcessId -ProcessName $WorkerProcessName

Write-Host "[*] Параметры запуска:" -ForegroundColor Cyan
Write-Host "    Worker PID:       $workerPid"
Write-Host "    Trace duration:   ${TraceDuration}s"
Write-Host "    GcDump at peak:   ${GcDumpAtPeakSec}s"
Write-Host "    Drain wait:       ${DrainWaitSec}s"
Write-Host "    Output dir:       $resolveDir"
Write-Host ""

# ============================================================
# 1. Запуск dotnet-trace в фоне
# ============================================================
$traceJob = Start-TraceCollection -ProcessId $workerPid -DurationSec $TraceDuration -OutputFilePath $traceFile

# ============================================================
# 2. Запуск сбора метрик в фоне
# ============================================================
$countersJob = Start-Job -ScriptBlock {
    param($url, $file, $interval)

    function Parse-Metrics {
        param($Text, $Timestamp)
        $results = [System.Collections.Generic.List[PSObject]]::new()
        $lines = $Text -split "`n"
        $currentHelp = ""; $currentType = ""

        foreach ($line in $lines) {
            $line = $line.Trim()
            if ([string]::IsNullOrWhiteSpace($line)) { continue }
            if ($line.StartsWith("# HELP ")) { $parts = $line.Substring(7) -split " ", 2; if ($parts.Count -ge 2) { $currentHelp = $parts[1] }; continue }
            if ($line.StartsWith("# TYPE ")) { $parts = $line.Substring(7) -split " ", 2; if ($parts.Count -ge 2) { $currentType = $parts[1] }; continue }
            if ($line.StartsWith("#")) { continue }

            if ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*)\{?(.*?)\}?\s+([\d.eE+\-]+|NaN|Inf|\+Inf|-Inf)') {
                $metricName = $Matches[1]; $labelsRaw = $Matches[2]; $value = $Matches[3]
                $labelsStr = ""
                if ($labelsRaw) {
                    $labelPairs = [System.Collections.Generic.List[string]]::new()
                    $m2 = [regex]::Matches($labelsRaw, '([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')
                    foreach ($m in $m2) { $labelPairs.Add("$($m.Groups[1].Value)=$($m.Groups[2].Value)") }
                    $labelsStr = $labelPairs -join "; "
                }
                $results.Add([PSCustomObject]@{ Timestamp=$Timestamp; Metric=$metricName; Labels=$labelsStr; Type=$currentType; Value=$value; Description=$currentHelp })
            } elseif ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+([\d.eE+\-]+|NaN|Inf|\+Inf|-Inf)') {
                $results.Add([PSCustomObject]@{ Timestamp=$Timestamp; Metric=$Matches[1]; Labels=""; Type=$currentType; Value=$Matches[2]; Description=$currentHelp })
            }
        }
        return , $results.ToArray()
    }

    # Проверка доступности Worker
    $retries = 0
    $connected = $false
    while (-not $connected -and $retries -lt 6) {
        try {
            $null = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
            $connected = $true
        } catch {
            $retries++
            if ($retries -ge 6) {
                Write-Output "[COUNTERS] Worker недоступен после $($retries) попыток: $($_.Exception.Message)"
                return
            }
            Start-Sleep -Seconds 3
        }
    }

    "Timestamp,Metric,Labels,Type,Value,Description" | Out-File -FilePath $file -Encoding UTF8

    $startTime = Get-Date
    $sampleCount = 0
    $errorCount = 0
    while ($true) {
        $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        try {
            $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 10
            $metrics = Parse-Metrics -Text $resp.Content -Timestamp $ts
            foreach ($m in $metrics) {
                $ed = $m.Description -replace '"', '""'
                "`"$($m.Timestamp)`",`"$($m.Metric)`",`"$($m.Labels)`",`"$($m.Type)`",$($m.Value),`"$ed`"" | Out-File -FilePath $file -Append -Encoding UTF8
            }
            $sampleCount++
            $elapsed = ((Get-Date) - $startTime).TotalSeconds
            Write-Output "[COUNTERS] Sample #$sampleCount | $($metrics.Count) metrics | $($elapsed.ToString('N0'))s elapsed"
        } catch {
            $errorCount++
            if ($errorCount -le 3) {
                Write-Output "[COUNTERS] Ошибка: $($_.Exception.Message)"
            } elseif ($errorCount -eq 4) {
                Write-Output "[COUNTERS] Further errors suppressed (count: $errorCount)"
            }
        }
        Start-Sleep -Seconds $interval
    }
} -ArgumentList $MetricsUrl, $csvFile, $RefreshSeconds

Write-Host "[*] Запуск сбора Prometheus-метрик (counters)..." -ForegroundColor Cyan
Write-Host "    URL:     $MetricsUrl"
Write-Host "    Output:  $csvFile"
Write-Host "    Interval: ${RefreshSeconds}s"
Write-Host "[+] Сбор метрик запущен в фоне (Job ID: $($countersJob.Id))" -ForegroundColor Green
Show-CountersJobProgress -Job $countersJob

# ============================================================
# 3. Ожидание пика нагрузки → gcdump #1
# ============================================================
WaitFor-PeakLoad -Seconds $GcDumpAtPeakSec
Show-CountersJobProgress -Job $countersJob
Collect-GcDump -ProcessId $workerPid -OutputPath $peakFile -Label "PEAK"

# ============================================================
# 4. Ожидание окончания trace
# ============================================================
$remaining = $TraceDuration - $GcDumpAtPeakSec
if ($remaining -gt 0) {
    Write-Host "[*] Ожидание завершения trace (ещё ${remaining}s)..." -ForegroundColor Cyan
    $elapsed = 0
    while ($elapsed -lt $remaining) {
        if ($traceJob.Process.HasExited) { break }
        Start-Sleep -Seconds 1
        $elapsed++
        if ($elapsed % 10 -eq 0) {
            Write-Host "    Осталось $($remaining - $elapsed)s..."
            Show-CountersJobProgress -Job $countersJob
        }
    }
}

# ============================================================
# 5. Остановка trace (с диагностикой ExitCode/nettrace)
# ============================================================
Stop-TraceCollection -TraceProcess $traceJob.Process -TraceJob $traceJob
if (-not (Test-Path $traceFile)) {
    Write-Host "[!] ВНИМАНИЕ: nettrace не создан: $traceFile" -ForegroundColor Red
}

# ============================================================
# 6. Ожидание дренажа → gcdump #2
# ============================================================
WaitFor-Drain -TimeoutSec $DrainWaitSec -MetricsEndpoint $MetricsUrl
Show-CountersJobProgress -Job $countersJob
Collect-GcDump -ProcessId $workerPid -OutputPath $drainedFile -Label "DRAINED"

# ============================================================
# 7. Конвертация trace → SpeedScope
# ============================================================
if (Test-Path $traceFile) {
    Write-Host "[*] Пауза 2 сек перед конвертацией (финализация .nettrace)..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 2
    Convert-TraceToSpeedScope -TraceFile $traceFile
}

# ============================================================
# 8. Остановка counters job
# ============================================================
Write-Host "[*] Остановка сбора метрик..." -ForegroundColor DarkGray
Show-CountersJobProgress -Job $countersJob
Stop-Job $countersJob -ErrorAction SilentlyContinue
Remove-Job $countersJob -ErrorAction SilentlyContinue

# ============================================================
# 9. Генерация сводного отчёта
# ============================================================
Write-Host "[*] Генерация сводного отчёта..." -ForegroundColor Cyan

$traceSize = if (Test-Path $traceFile) { "$([math]::Round((Get-Item $traceFile).Length/1MB, 2)) MB" } else { "N/A" }
$speedScopeSize = if (Test-Path ([System.IO.Path]::ChangeExtension($traceFile, ".speedscope.json"))) { "$([math]::Round((Get-Item ([System.IO.Path]::ChangeExtension($traceFile, ".speedscope.json"))).Length/1MB, 2)) MB" } else { "N/A" }
$peakSize = if (Test-Path $peakFile) { "$([math]::Round((Get-Item $peakFile).Length/1MB, 2)) MB" } else { "N/A" }
$drainedSize = if (Test-Path $drainedFile) { "$([math]::Round((Get-Item $drainedFile).Length/1MB, 2)) MB" } else { "N/A" }
$csvSize = if (Test-Path $csvFile) { "$([math]::Round((Get-Item $csvFile).Length/1MB, 2)) MB" } else { "N/A" }

# Предупреждения об аномалиях сбора
$reportWarnings = @()
if (-not (Test-Path $traceFile)) {
    $reportWarnings += "- nettrace НЕ создан ($traceFile). Трассировка аллокаций недоступна."
}
if (-not (Test-Path ([System.IO.Path]::ChangeExtension($traceFile, ".speedscope.json")))) {
    $reportWarnings += "- speedscope.json НЕ создан. Визуализация трассы недоступна."
}

$reportContent = @"
# Profiling Report — $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

## Configuration

| Параметр | Значение |
|----------|----------|
| Mode | all |
| Trace Duration | ${TraceDuration}s |
| GcDump at Peak | ${GcDumpAtPeakSec}s |
| Drain Wait | ${DrainWaitSec}s |

## Output Files

| Файл | Размер |
|------|--------|
| allocation_trace_$timestamp.nettrace | $traceSize |
| allocation_trace_$timestamp.speedscope.json | $speedScopeSize |
| snapshot_peak_$timestamp.gcdump | $peakSize |
| snapshot_drained_$timestamp.gcdump | $drainedSize |
| counters_$timestamp.csv | $csvSize |

$(if ($reportWarnings.Count -gt 0) {
    @"
## Предупреждения сбора

$($reportWarnings -join "`n")
"@
})

## Анализ

### dotnet-trace
- Откройте `.speedscope.json` в https://www.speedscope.app
- Выберите профиль `GCAllocationTick` (или `GCSampledObjectAllocation`)
- Ищите широкие "плато" — горячие методы с аллокациями
- Обратите особое внимание на `System.String`, `byte[]`, `KeyValuePair<,>`

### dotnet-gcdump
- Откройте `.gcdump` файлы в Visual Studio (Debug → Memory Usage → Load Snapshot)
- Сравните **snapshot_peak** vs **snapshot_drained**
- Ключевые метрики:
  - Gen2 fragmentation %
  - LOH fragmentation %
  - Top types by count
  - Top types by size
  - Survivor ratio

## Next Steps

1. Если `string` аллокации > 40% — смотреть decimal.ToString/Parse в горячем пути
2. Если `KeyValuePair` > 5% — оптимизировать OTel теги
3. Если Gen2 фрагментация > 15% — нужен compacting GC
4. Если `TickData` count > 50K в peak — возможно, нужно увеличить ChannelCapacity
"@

$reportContent | Out-File -FilePath $reportFile -Encoding UTF8
Write-Host "[+] Сводный отчёт: $reportFile" -ForegroundColor Green

# ============================================================
# 10. Итог
# ============================================================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  ПРОФИЛИРОВАНИЕ ЗАВЕРШЕНО" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Результаты в: $resolveDir" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Файлы для анализа:" -ForegroundColor DarkGray
if (Test-Path ([System.IO.Path]::ChangeExtension($traceFile, ".speedscope.json"))) {
    Write-Host "    SpeedScope:  https://www.speedscope.app#file=$(Resolve-Path ([System.IO.Path]::ChangeExtension($traceFile, ".speedscope.json")))"
}
Write-Host "    Peak GCDump: $peakFile"
Write-Host "    Drained:     $drainedFile"
if (Test-Path $csvFile) {
    Write-Host "    Metrics CSV: $csvFile"
}
Write-Host "    Report:      $reportFile"
Write-Host ""
