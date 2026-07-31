# Оркестратор нагрузочного прогона для проверки lock contention
# после батчевого сбора метрик (CounterBatcher).
#
# Отличие от первой версии: сбор метрик стартует параллельно с Worker
# (фоновый Job), прогон удлинён (4M тиков) для захвата нескольких скрейпов
# Prometheus и корректного фиксирования lock_contention_count_total.
$ErrorActionPreference = "Stop"

$traces = "d:/work/MarketDataCollector/traces"
New-Item -ItemType Directory -Path $traces -Force | Out-Null

# Чистим порт 5000 и 5010 от возможных остатков
taskkill /F /IM FakeTickServer.exe 2>$null
taskkill /F /IM MarketDataCollector.Worker.exe 2>$null
Start-Sleep -Seconds 2

Write-Host "=== [1/4] Компиляция FakeTickServer ===" -ForegroundColor Cyan
Push-Location d:/work/MarketDataCollector/tests/FakeTickServer
dotnet build -c Debug | Out-Host
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "FakeTickServer build failed" }
Pop-Location

Write-Host "=== [2/4] Запуск FakeTickServer (4M, 25k RPS) ===" -ForegroundColor Cyan
$fakeServer = Start-Process dotnet -ArgumentList @(
    "run","--project","d:/work/MarketDataCollector/tests/FakeTickServer",
    "--","--port","5000","--rps","25000","--symbols","btcusdt,ethusdt,solusdt",
    "--base-price","5000","--max-ticks","4000000","--dup-percent","3"
) -PassThru -RedirectStandardOutput "$traces/fake_server_out2.log" -RedirectStandardError "$traces/fake_server_err2.log"
Write-Host "    FakeTickServer PID: $($fakeServer.Id)" -ForegroundColor Green

Write-Host "Ожидание готовности fake-сервера (8с)..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

Write-Host "=== [3/4] Запуск Worker ===" -ForegroundColor Cyan
Push-Location d:/work/MarketDataCollector/src/MarketDataCollector.Workers/MarketDataCollector.Worker
$worker = Start-Process dotnet -ArgumentList "run","-c","Debug" -PassThru `
    -RedirectStandardOutput "$traces/worker_out2.log" `
    -RedirectStandardError "$traces/worker_err2.log"
Pop-Location
Write-Host "    Worker PID: $($worker.Id)" -ForegroundColor Green

# === [4/4] Параллельный сбор метрик (фоновый Job) ===
Write-Host "=== [4/4] Сбор метрик contention (фоновый Job) ===" -ForegroundColor Cyan
$metricsUrl = "http://localhost:5010/metrics"
$csv = Join-Path $traces "contention_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"
"timestamp,lock_contention_count_total,ticks_incoming_total,ws_messages_received_total,drops_total" | Out-File $csv -Encoding utf8

$job = Start-Job -ScriptBlock {
    param($url, $csvPath)
    function Get-MetricValue([string]$body, [string]$name) {
        foreach ($line in ($body -split "`n")) {
            if ($line.StartsWith($name)) {
                return ($line -split "\s+")[1]
            }
        }
        return ""
    }
    $deadline = (Get-Date).AddMinutes(5)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 10
            $text = $resp.Content
            $cont = Get-MetricValue $text "lock_contention_count_total"
            $inc  = Get-MetricValue $text "ticks_incoming_total"
            $ws   = Get-MetricValue $text "ws_messages_received_total"
            $drop = Get-MetricValue $text "ticks_dropped_total"
            "$(Get-Date -Format 'HH:mm:ss'),$cont,$inc,$ws,$drop" | Out-File $csvPath -Append -Encoding utf8
            Write-Host "    $((Get-Date).ToString('HH:mm:ss')) contention=$cont incoming=$inc ws=$ws drops=$drop"
        }
        catch {
            Write-Host "    [warn] $((Get-Date).ToString('HH:mm:ss')): $($_.Exception.Message)" -ForegroundColor DarkYellow
        }
        Start-Sleep -Seconds 10
    }
} -ArgumentList $metricsUrl, $csv

Write-Host "Сбор запущен. Ожидание завершения генерации тиков (~90с)..." -ForegroundColor Yellow
Start-Sleep -Seconds 120

Write-Host "=== Итог ===" -ForegroundColor Cyan
Stop-Job $job -ErrorAction SilentlyContinue
Receive-Job $job -ErrorAction SilentlyContinue
Remove-Job $job -Force -ErrorAction SilentlyContinue
Write-Host "CSV сохранён: $csv" -ForegroundColor Green
Write-Host "FakeTickServer PID: $($fakeServer.Id)  Worker PID: $($worker.Id)" -ForegroundColor Gray
