<#
.SYNOPSIS
    Сбор Prometheus-метрик MarketDataCollector.Worker в CSV.

.DESCRIPTION
    Опрашивает /metrics endpoint Worker и записывает все метрики в CSV-файл.
    Поддерживает ручную остановку (любая клавиша) и автоматическую по таймеру.

.PARAMETER MetricsUrl
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.

.PARAMETER OutputDir
    Директория для CSV-файла. По умолчанию ./traces.

.PARAMETER RefreshSeconds
    Интервал опроса метрик в секундах. По умолчанию 5.

.PARAMETER Duration
    Максимальная длительность сбора в секундах. 0 = без ограничений.

.EXAMPLE
    .\scripts\collect-counters.ps1

    .\scripts\collect-counters.ps1 -MetricsUrl "http://localhost:5010/metrics" -Duration 120

    .\scripts\collect-counters.ps1 -OutputDir "./my-traces" -RefreshSeconds 10
#>

param(
    [string]$MetricsUrl = "http://localhost:5010/metrics",
    [string]$OutputDir = "./traces",
    [int]$RefreshSeconds = 5,
    [int]$Duration = 0
)

$ErrorActionPreference = "Stop"

# Подключаем общие функции
. "$PSScriptRoot\common-functions.ps1"

Write-SectionHeader "Сбор Prometheus-метрик"

# ============================================================
# 1. Проверяем доступность Worker
# ============================================================
Write-Host "[*] Checking Worker at $MetricsUrl ..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri $MetricsUrl -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -ne 200) {
        Write-Host "[!] Worker returned HTTP $($response.StatusCode)" -ForegroundColor Red
        return
    }
    $metricCount = ($response.Content -split "`n" | Where-Object { $_ -match "^[a-z_]" }).Count
    Write-Host "[+] Worker is alive. Found $metricCount metric entries in Prometheus format." -ForegroundColor Green
} catch {
    Write-Host "[!] Cannot reach Worker at $MetricsUrl" -ForegroundColor Red
    Write-Host "    $($_.Exception.Message)" -ForegroundColor Yellow
    Write-Host "    Is MarketDataCollector.Worker running?" -ForegroundColor Yellow
    return
}

# ============================================================
# 2. Готовим директорию и имя файла
# ============================================================
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$outputFile = Join-Path (Resolve-Path $OutputDir).Path "counters_$timestamp.csv"

# ============================================================
# 3. Определяем, доступен ли ввод с клавиатуры
# ============================================================
$hasConsole = $false
try {
    if ([Console]::IsInputRedirected -eq $false) {
        $null = [Console]::KeyAvailable
        $hasConsole = $true
    }
} catch {
    $hasConsole = $false
}

# ============================================================
# 4. Основной цикл сбора
# ============================================================
Write-Host ""
Write-Host "[+] Collecting metrics from $MetricsUrl" -ForegroundColor Cyan
Write-Host "    Output: $outputFile"
Write-Host "    Interval: ${RefreshSeconds}s"
if ($Duration -gt 0) {
    Write-Host "    Duration: ${Duration}s (auto-stop)" -ForegroundColor DarkGray
}
Write-Host ""

"Timestamp,Metric,Labels,Type,Value,Description" | Out-File -FilePath $outputFile -Encoding UTF8

$sampleCount = 0
$startTime = Get-Date

if ($hasConsole) {
    Write-Host "  Press any key to stop collection (or Ctrl+C)..." -ForegroundColor Yellow
} else {
    Write-Host "  Collecting in background mode..." -ForegroundColor Yellow
}

$collecting = $true
while ($collecting) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    try {
        $resp = Invoke-WebRequest -Uri $MetricsUrl -UseBasicParsing -TimeoutSec 10
        $metrics = Parse-PrometheusMetrics -Text $resp.Content -Timestamp $ts

        foreach ($m in $metrics) {
            $escapedDesc = ($m.Description -replace '"', '""')
            $csvLine = "`"$($m.Timestamp)`",`"$($m.Metric)`",`"$($m.Labels)`",`"$($m.Type)`",$($m.Value),`"$escapedDesc`""
            $csvLine | Out-File -FilePath $outputFile -Append -Encoding UTF8
        }

        $sampleCount++
        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        Write-Host "`r    [$sampleCount samples, $($elapsed.ToString('N0'))s elapsed, $($metrics.Count) metrics per sample] " -NoNewline -ForegroundColor DarkGray
    } catch {
        Write-Host "`r    [!] Error: $($_.Exception.Message) " -NoNewline -ForegroundColor Red
    }

    # Проверяем условие остановки
    if ($Duration -gt 0) {
        $totalElapsed = ((Get-Date) - $startTime).TotalSeconds
        if ($totalElapsed -ge $Duration) {
            Write-Host ""
            Write-Host "[+] Duration limit reached (${Duration}s)." -ForegroundColor Cyan
            break
        }
    }

    # Ждём до следующего опроса, проверяя нажатие клавиш (если консоль доступна)
    $waited = 0
    while ($waited -lt $RefreshSeconds) {
        if ($hasConsole -and [Console]::KeyAvailable) {
            [Console]::ReadKey($true) | Out-Null
            $collecting = $false
            break
        }
        Start-Sleep -Milliseconds 250
        $waited += 0.25
    }
}

Write-Host ""

# ============================================================
# 5. Валидация результата
# ============================================================
$lineCount = (Get-Content $outputFile | Measure-Object -Line).Lines
$dataLines = $lineCount - 1
$elapsed = ((Get-Date) - $startTime).TotalSeconds

if ($dataLines -le 0) {
    Write-Host "[!] WARNING: No metric data collected." -ForegroundColor Yellow
} else {
    Write-Host "[+] Done. Output: $outputFile" -ForegroundColor Green
    Write-Host "    Samples: $sampleCount | Data rows: $dataLines | Duration: $($elapsed.ToString('N0'))s" -ForegroundColor Green
}
