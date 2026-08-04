<#
.SYNOPSIS
    Однокнопочный оркестратор нагрузочного тестирования с профилированием.

.DESCRIPTION
    Запускает последовательность: FakeTickServer -> MarketDataCollector.Worker -> Profiler.
    Все артефакты (trace, gcdump x2, counters CSV, логи) собираются в ./traces.
    Обеспечивает корректное (graceful) завершение всех трёх сервисов:
      - FakeTickServer сам завершается по достижении MaxTicks;
      - Worker дренажит очередь, затем останавливается через POST /shutdown.

    Требует запущенный Docker (Postgres на порту 5433). Worker использует
    профиль конфига LoadTest (appsettings.LoadTest.json) — все клиенты идут
    только на локальный FakeTickServer, Kafka отключена.

.PARAMETER MaxTicks
    Количество тиков для FakeTickServer. По умолчанию 4000000.

.PARAMETER Rps
    Целевой RPS генератора. По умолчанию 25000.

.PARAMETER Symbols
    Список символов через запятую. По умолчанию btcusdt,ethusdt,solusdt.

.PARAMETER DupPercent
    Процент дублей (0-100). По умолчанию 3.

.PARAMETER TraceProfile
    Профиль dotnet-trace: gc-verbose, cpu-sampling, contention, contention-cpu.

.PARAMETER TraceDuration
    Длительность trace, секунд. По умолчанию 90.

.PARAMETER GcDumpAtPeakSec
    Момент первого gcdump (пик), секунд. По умолчанию 50.

.PARAMETER OutputDir
    Директория результатов. По умолчанию ./traces.

.PARAMETER SkipProfiler
    Пропустить профилирование (только прогнать нагрузку и остановить сервисы).

.EXAMPLE
    .\run_loadtest.ps1
    .\run_loadtest.ps1 -MaxTicks 2000000 -Rps 15000 -TraceProfile contention-cpu
#>

[CmdletBinding()]
param(
    [int]$MaxTicks = 1700000,
    [int]$Rps = 25000,
    [string]$Symbols = "btcusdt,ethusdt,solusdt",
    [int]$DupPercent = 3,

    [ValidateSet("gc-verbose", "cpu-sampling", "contention", "contention-cpu")]
    [string]$TraceProfile = "contention-cpu",
    [int]$TraceDuration = 90,
    [int]$GcDumpAtPeakSec = 50,

    [string]$OutputDir = "./traces",
    [switch]$SkipProfiler
)

# ============================================================
# Параметры запуска (выводятся в самом начале)
# ============================================================
Write-Host ""
Write-Host "---- Параметры запуска ----" -ForegroundColor Cyan
Write-Host "  MaxTicks:        $MaxTicks"
Write-Host "  Rps:             $Rps"
Write-Host "  Symbols:         $Symbols"
Write-Host "  DupPercent:      $DupPercent%"
Write-Host "  TraceProfile:    $TraceProfile"
Write-Host "  TraceDuration:   $TraceDuration с"
Write-Host "  GcDumpAtPeakSec: $GcDumpAtPeakSec с"
Write-Host "  OutputDir:       $OutputDir"
Write-Host "  SkipProfiler:    $(if ($SkipProfiler) { 'True' } else { 'False' })"
Write-Host "-----------------------------" -ForegroundColor Cyan
Write-Host ""

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

# ============================================================
# UTF-8 для корректного отображения кириллицы в выводе
# дочерних процессов (dotnet build пишет в cp866).
# ============================================================
try {
    chcp 65001 > $null
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
} catch {
    # Некритично, продолжаем выполнение
}

# ============================================================
# Вспомогательные функции
# ============================================================
function Write-Step([string]$Title) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
}

function Wait-Http([string]$Url, [int]$TimeoutSec, [string]$Label) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
            if ($resp.StatusCode -lt 500) {
                Write-Host "    $Label готов ($($resp.StatusCode))." -ForegroundColor Green
                return $true
            }
        }
        catch {
            # сервис ещё не готов — продолжаем поллинг
        }
        Start-Sleep -Seconds 2
    }
    Write-Host "    Таймаут ожидания $Label." -ForegroundColor Red
    return $false
}

function Wait-ProcessExit([System.Diagnostics.Process]$proc, [int]$TimeoutSec, [string]$Label) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $proc.Refresh()
        if ($proc.HasExited) {
            Write-Host "    $Label завершился (код $($proc.ExitCode))." -ForegroundColor Green
            return $true
        }
        Start-Sleep -Seconds 2
    }
    Write-Host "    Таймаут ожидания завершения $Label." -ForegroundColor Red
    return $false
}

# ============================================================
# Preflight
# ============================================================
Write-Step "[1/7] Preflight"

# Docker / Postgres
$dockerRunning = docker info 2>&1 | Select-String "Server Version"
if (-not $dockerRunning) {
    Write-Host "  ВНИМАНИЕ: Docker не запущен. Worker не сможет подключиться к Postgres (порт 5433)." -ForegroundColor Yellow
    Write-Host "  Запустите: docker-compose -f docker/docker-compose.yml up -d" -ForegroundColor Yellow
}
else {
    Write-Host "  Docker запущен." -ForegroundColor Green
}

# Очистка остатков предыдущих прогонов
Write-Host "  Очистка остатков процессов..."
taskkill /F /IM FakeTickServer.exe 2>$null
taskkill /F /IM MarketDataCollector.Worker.exe 2>$null
taskkill /F /IM dotnet-trace.exe 2>$null
Start-Sleep -Seconds 2

# ============================================================
# Компиляция
# ============================================================
Write-Step "[2/7] Компиляция"

Write-Host "  Сборка FakeTickServer..."
Push-Location "$root/tests/FakeTickServer"
dotnet build -c Debug --nologo | Out-Host
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "FakeTickServer build failed" }
Pop-Location

Write-Host "  Сборка решения (Worker)..."
Push-Location $root
dotnet build MarketDataCollector.sln -c Debug --nologo | Out-Host
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "Solution build failed" }
Pop-Location

Write-Host "  Сборка Profiler..."
Push-Location "$root/tools/Profiler"
dotnet build -c Debug --nologo | Out-Host
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "Profiler build failed" }
Pop-Location

# ============================================================
# Запуск FakeTickServer
# ============================================================
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
$fakeExe = "$root/tests/FakeTickServer/bin/Debug/net8.0/FakeTickServer.exe"
$fakeOut = Join-Path $root "$OutputDir/fake_server_out.log"
$fakeErr = Join-Path $root "$OutputDir/fake_server_err.log"

Write-Step "[3/7] Запуск FakeTickServer (:5000)"
$fakeArgs = @(
    "--port", "5000",
    "--rps", "$Rps",
    "--symbols", "$Symbols",
    "--base-price", "5000",
    "--max-ticks", "$MaxTicks",
    "--dup-percent", "$DupPercent"
)
$fakeProc = Start-Process -FilePath $fakeExe -ArgumentList $fakeArgs -PassThru `
    -RedirectStandardOutput $fakeOut -RedirectStandardError $fakeErr -NoNewWindow
Write-Host "  FakeTickServer PID: $($fakeProc.Id), лог: $fakeOut" -ForegroundColor Green

if (-not (Wait-Http "http://localhost:5000/health" 30 "FakeTickServer")) {
    taskkill /F /IM FakeTickServer.exe 2>$null
    throw "FakeTickServer не поднялся"
}

# ============================================================
# Запуск Worker (профиль LoadTest)
# ============================================================
$workerExe = "$root/src/MarketDataCollector.Workers/MarketDataCollector.Worker/bin/Debug/net8.0/MarketDataCollector.Worker.exe"
$workerOut = Join-Path $root "$OutputDir/worker_out.log"
$workerErr = Join-Path $root "$OutputDir/worker_err.log"

Write-Step "[4/7] Запуск Worker (:5010, профиль LoadTest)"
# Профиль LoadTest активируется через ASPNETCORE_ENVIRONMENT (WebApplication).
$env:ASPNETCORE_ENVIRONMENT = "LoadTest"

# Валидация рабочей директории: для скомпилированного .exe ContentRoot = текущая
# рабочая директория процесса. Если она != каталогу вывода, appsettings*.json не
# подхватятся (известная причина "Connection string 'MarketDataDb' is not configured").
$workerWorkDir = Split-Path $workerExe
$hasBaseAppSettings = Test-Path (Join-Path $workerWorkDir "appsettings.json")
$hasLoadTestAppSettings = Test-Path (Join-Path $workerWorkDir "appsettings.LoadTest.json")
$currentDir = (Get-Location).Path
Write-Host "  [debug] WorkerExe:       $workerExe"
Write-Host "  [debug] WorkerWorkDir:   $workerWorkDir"
Write-Host "  [debug] CurrentDir:      $currentDir"
Write-Host "  [debug] appsettings.json exists: $hasBaseAppSettings"
Write-Host "  [debug] appsettings.LoadTest.json exists: $hasLoadTestAppSettings"
if (-not $hasBaseAppSettings -or -not $hasLoadTestAppSettings) {
    Write-Host "  ВНИМАНИЕ: appsettings.json не найден в рабочей директории Worker." -ForegroundColor Yellow
    Write-Host "  Worker, вероятно, упадёт с 'Connection string MarketDataDb is not configured'." -ForegroundColor Yellow
}

$workerProc = Start-Process -FilePath $workerExe -ArgumentList @("--no-launch-profile") -WorkingDirectory $workerWorkDir -PassThru `
    -RedirectStandardOutput $workerOut -RedirectStandardError $workerErr -NoNewWindow
$env:ASPNETCORE_ENVIRONMENT = ""
Write-Host "  Worker PID: $($workerProc.Id), лог: $workerOut" -ForegroundColor Green

if (-not (Wait-Http "http://localhost:5010/health" 60 "Worker")) {
    taskkill /F /IM FakeTickServer.exe 2>$null
    taskkill /F /IM MarketDataCollector.Worker.exe 2>$null
    throw "Worker не поднялся (проверьте $workerErr)"
}

# ============================================================
# Профилирование (Profiler)
# ============================================================
if ($SkipProfiler) {
    Write-Host ""
    Write-Host "Пропуск Profiler (-SkipProfiler)." -ForegroundColor Yellow
}
else {
    Write-Step "[5/7] Профилирование (Profiler: $TraceProfile, $TraceDuration с)"
    # Передаём именованные параметры явно: splatting обычного массива строк
    # передаёт элементы позиционно, из-за чего "-TraceProfile" попадал как
    # ЗНАЧЕНИЕ параметра TraceProfile и не проходил ValidateSet.
    & "$root/run_all_profiler.ps1" `
        -TraceProfile $TraceProfile `
        -TraceDuration $TraceDuration `
        -GcDumpAtPeakSec $GcDumpAtPeakSec `
        -OutputDir $OutputDir
    Write-Host "  Profiler завершён." -ForegroundColor Green
}

# ============================================================
# Ожидание самозавершения FakeTickServer (по MaxTicks)
# ============================================================
Write-Step "[6/7] Ожидание завершения FakeTickServer"
# FakeServer генерирует MaxTicks/Rps секунд; после достижения лимита сам
# завершает хост (StopApplication). Даём запас на дренаж Worker'а.
$maxSeconds = [Math]::Max(60, [int]($MaxTicks / [Math]::Max(1, $Rps)) + 120)
$fakeProc.Refresh()
if (-not $fakeProc.HasExited) {
    if (Wait-ProcessExit $fakeProc $maxSeconds "FakeTickServer") {
        # FakeServer закончил подачу данных. Даём Worker'у время дренажить очередь.
        Write-Host "  Пауза для дренажа очередей Worker (15с)..." -ForegroundColor Yellow
        Start-Sleep -Seconds 15
    }
    else {
        Write-Host "  FakeTickServer не завершился за $maxSeconds с — принудительная остановка." -ForegroundColor Yellow
        taskkill /F /IM FakeTickServer.exe 2>$null
    }
}

# ============================================================
# Graceful остановка Worker
# ============================================================
Write-Step "[7/7] Graceful остановка Worker (/shutdown)"
try {
    $shutdownResp = Invoke-RestMethod -Uri "http://localhost:5010/shutdown" -Method Post -TimeoutSec 10
    Write-Host "  /shutdown -> $($shutdownResp.status)" -ForegroundColor Green
}
catch {
    Write-Host "  Ошибка POST /shutdown: $($_.Exception.Message)" -ForegroundColor Yellow
}

$workerProc.Refresh()
if (-not $workerProc.HasExited) {
    if (Wait-ProcessExit $workerProc 90 "Worker") {
        # ok
    }
    else {
        Write-Host "  Worker не завершился за 90 с — принудительная остановка." -ForegroundColor Yellow
        taskkill /F /IM MarketDataCollector.Worker.exe 2>$null
    }
}

# ============================================================
# Итоговая сводка
# ============================================================
Write-Step "Итог"
$artifacts = Get-ChildItem -Path (Join-Path $root $OutputDir) -File -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 15

Write-Host "  Артефакты в: $(Join-Path $root $OutputDir)" -ForegroundColor Cyan
foreach ($f in $artifacts) {
    $sizeMb = [Math]::Round($f.Length / 1MB, 2)
    Write-Host ("  {0,-45} {1,10} MB  {2}" -f $f.Name, $sizeMb, $f.LastWriteTime.ToString("HH:mm:ss")) -ForegroundColor Gray
}

Write-Host ""
Write-Host "Прогон завершён. Логи FakeServer/Worker: $OutputDir\fake_server_*.log, $OutputDir\worker_*.log" -ForegroundColor Green
