<#
.SYNOPSIS
    Собирает метрики MarketDataCollector.Worker через Prometheus /metrics endpoint.
.DESCRIPTION
    Периодически опрашивает http://localhost:5010/metrics и записывает в CSV.
    Нажмите любую клавишу для остановки (или Ctrl+C).
.PARAMETER OutputDir
    Директория для файла с метриками. По умолчанию ./counters.
.PARAMETER RefreshSeconds
    Интервал опроса метрик в секундах. По умолчанию 5.
.PARAMETER Url
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.
.PARAMETER Duration
    Максимальная длительность сбора в секундах. 0 = без ограничений (ждать клавишу). По умолчанию 0.
#>

param(
    [string]$OutputDir = "./counters",
    [int]$RefreshSeconds = 5,
    [string]$Url = "http://localhost:5010/metrics",
    [int]$Duration = 0
)

$ErrorActionPreference = "Stop"

# ============================================================
# 1. Проверяем доступность Worker
# ============================================================
Write-Host "[*] Checking Worker at $Url ..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -ne 200) {
        Write-Host "[!] Worker returned HTTP $($response.StatusCode)" -ForegroundColor Red
        exit 1
    }
    $metricCount = ($response.Content -split "`n" | Where-Object { $_ -match "^[a-z_]" }).Count
    Write-Host "[+] Worker is alive. Found $metricCount metric entries in Prometheus format." -ForegroundColor Green
} catch {
    Write-Host "[!] Cannot reach Worker at $Url" -ForegroundColor Red
    Write-Host "    $($_.Exception.Message)" -ForegroundColor Yellow
    Write-Host "    Is MarketDataCollector.Worker running?" -ForegroundColor Yellow
    exit 1
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
# 3. Парсим Prometheus text format → CSV
# ============================================================
function Parse-PrometheusMetrics {
    param([string]$Text, [string]$Timestamp)

    $results = [System.Collections.Generic.List[PSObject]]::new()
    $lines = $Text -split "`n"
    $currentHelp = ""
    $currentType = ""

    foreach ($line in $lines) {
        $line = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($line)) { continue }

        if ($line.StartsWith("# HELP ")) {
            $parts = $line.Substring(7) -split " ", 2
            if ($parts.Count -ge 2) { $currentHelp = $parts[1] }
            continue
        }
        if ($line.StartsWith("# TYPE ")) {
            $parts = $line.Substring(7) -split " ", 2
            if ($parts.Count -ge 2) { $currentType = $parts[1] }
            continue
        }
        if ($line.StartsWith("#")) { continue }

        # Data line with labels: metric_name{labels} value
        if ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*)\{?(.*?)\}?\s+([\d.eE+\-]+|NaN|Inf|\+Inf|-Inf)') {
            $metricName = $Matches[1]
            $labelsRaw = $Matches[2]
            $value = $Matches[3]

            $labelsStr = ""
            if ($labelsRaw) {
                $labelPairs = [System.Collections.Generic.List[string]]::new()
                $labelMatches = [regex]::Matches($labelsRaw, '([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')
                foreach ($m in $labelMatches) {
                    $labelPairs.Add("$($m.Groups[1].Value)=$($m.Groups[2].Value)")
                }
                $labelsStr = $labelPairs -join "; "
            }

            $results.Add([PSCustomObject]@{
                Timestamp   = $Timestamp
                Metric      = $metricName
                Labels      = $labelsStr
                Type        = $currentType
                Value       = $value
                Description = $currentHelp
            })
        }
        # Data line without labels: metric_name value
        elseif ($line -match '^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+([\d.eE+\-]+|NaN|Inf|\+Inf|-Inf)') {
            $results.Add([PSCustomObject]@{
                Timestamp   = $Timestamp
                Metric      = $Matches[1]
                Labels      = ""
                Type        = $currentType
                Value       = $Matches[2]
                Description = $currentHelp
            })
        }
    }

    return , $results.ToArray()
}

# ============================================================
# 4. Определяем, доступен ли ввод с клавиатуры
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
# 5. Основной цикл сбора
# ============================================================
Write-Host ""
Write-Host "[+] Collecting metrics from $Url" -ForegroundColor Cyan
Write-Host "    Output: $outputFile"
Write-Host "    Interval: ${RefreshSeconds}s"
if ($Duration -gt 0) {
    Write-Host "    Duration: ${Duration}s (auto-stop)" -ForegroundColor DarkGray
}
Write-Host ""

# Записываем заголовок CSV
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
        $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 10
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
# 6. Валидация результата
# ============================================================
$lineCount = (Get-Content $outputFile | Measure-Object -Line).Lines
$dataLines = $lineCount - 1  # минус заголовок
$elapsed = ((Get-Date) - $startTime).TotalSeconds

if ($dataLines -le 0) {
    Write-Host "[!] WARNING: No metric data collected." -ForegroundColor Yellow
} else {
    Write-Host "[+] Done. Output: $outputFile" -ForegroundColor Green
    Write-Host "    Samples: $sampleCount | Data rows: $dataLines | Duration: $($elapsed.ToString('N0'))s" -ForegroundColor Green
}
