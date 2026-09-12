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
    .\start_loadtest.ps1
    .\start_loadtest.ps1 -MaxTicks 2000000 -Rps 15000 -TraceProfile contention-cpu
#>

[CmdletBinding()]
param(
    [int]$MaxTicks = 1700000,
    [int]$Rps = 25000,
    [string]$Symbols = "btcusdt,ethusdt,solusdt",
    [int]$DupPercent = 3,

    [ValidateSet("gc-verbose", "cpu-sampling", "contention", "contention-cpu")]
    [string]$TraceProfile = "cpu-sampling",
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
            # HTTP >= 500 (например, 503 degraded от health-чека Worker при wsAllDown):
            # сервис поднят, но readiness-чек считает его неготовым. Логируем код и тело,
            # чтобы отличить "degraded" от реальной недоступности.
            Write-Host "    ${Label}: HTTP $($resp.StatusCode) - сервис поднят, но readiness!=OK (degraded)." -ForegroundColor Yellow
        }
        catch {
            # сервис ещё не готов / соединение недоступно - продолжаем поллинг
            Write-Host "    ${Label}: запрос не прошёл: $($_.Exception.Message)" -ForegroundColor Gray
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

function Wait-GenerationComplete([string]$HealthUrl, [int]$MaxTicks, [int]$Rps) {
    # Polling /health с проверкой isLimitReached вместо blind sleep.
    # Таймаут = расчётное время генерации * 2, но не меньше 60с.
    $estSec = [Math]::Max(30, [int]($MaxTicks / [Math]::Max(1, $Rps)))
    $timeoutSec = [Math]::Max(60, $estSec * 2)
    $deadline = (Get-Date).AddSeconds($timeoutSec)

    Write-Host "  Ожидание генерации $MaxTicks тиков (polling $HealthUrl, таймаут ${timeoutSec}с)..."

    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-RestMethod -Uri $HealthUrl -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
            if ($resp.isLimitReached -eq $true) {
                Write-Host "  Генерация завершена: $($resp.totalTicks) тиков, isLimitReached=true." -ForegroundColor Green
                return $true
            }
            Write-Host "  Генерация: $($resp.totalTicks) / $MaxTicks тиков, клиентов: $($resp.clients), статус: $($resp.status)" -ForegroundColor Gray
        }
        catch {
            Write-Host "  Health-запрос не удался: $($_.Exception.Message)" -ForegroundColor Yellow
        }
        Start-Sleep -Seconds 3
    }
    Write-Host "  Таймаут ожидания генерации (${timeoutSec}с)." -ForegroundColor Red
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
# Профилирование — Phase 1 (Profiler, background)
# ============================================================
$profilerProc = $null
if (-not $SkipProfiler) {
    Write-Step "[5/7] Профилирование Phase 1 (Profiler: $TraceProfile, $TraceDuration с)"

    $profilerExe = "$root/tools/Profiler/bin/Debug/net8.0/MarketDataCollector.Profiler.exe"
    if (-not (Test-Path $profilerExe)) {
        Write-Host "  Предупреждение: Profiler не собран — пропуск." -ForegroundColor Yellow
        Write-Host "  Соберите: .\tools\Profiler\compile.ps1" -ForegroundColor Yellow
    }
    else {
        $profilerLog = Join-Path $root "$OutputDir/profiler_out.log"
        $profilerErr = Join-Path $root "$OutputDir/profiler_err.log"

        # Запускаем Profiler в фоне — он выполнит Phase 1 (trace + peak gcdump),
        # затем откроет HTTP :5100 и будет ждать сигнала /trigger-drained-collect
        $profilerProc = Start-Process -FilePath $profilerExe -PassThru `
            -RedirectStandardOutput $profilerLog -RedirectStandardError $profilerErr -NoNewWindow `
            -ArgumentList @(
                "--trace-profile", "$TraceProfile",
                "--trace-duration", "$TraceDuration",
                "--gc-dump-at-peak-sec", "$GcDumpAtPeakSec",
                "--output-dir", "$OutputDir"
            )
        Write-Host "  Profiler PID: $($profilerProc.Id), лог: $profilerLog" -ForegroundColor Green

        # Ждём, пока Profiler HTTP-сервер не покажет phase=awaiting-drain-signal
        Write-Host "  Ожидание Phase 1 (awaiting-drain-signal)..." -ForegroundColor Yellow
        $phase1Deadline = (Get-Date).AddSeconds(120)
        $phase1Ready = $false
        while ((Get-Date) -lt $phase1Deadline) {
            try {
                $status = Invoke-RestMethod -Uri "http://localhost:5100/status" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
                if ($status.phase -eq "awaiting-drain-signal") {
                    Write-Host "  Phase 1 завершена, ожидание сигнала дренажа." -ForegroundColor Green
                    $phase1Ready = $true
                    break
                }
                Write-Host "  Phase 1: $($status.phase)..." -ForegroundColor Gray
            }
            catch {
                Write-Host "  Ожидание Phase 1..." -ForegroundColor Gray
            }
            Start-Sleep -Seconds 3
        }

        if (-not $phase1Ready) {
            Write-Host "  Предупреждение: Phase 1 не подтверждена за 120с — продолжаем." -ForegroundColor Yellow
        }
    }
}
else {
    Write-Host "Пропуск Profiler (-SkipProfiler)." -ForegroundColor Yellow
}

# ============================================================
# Ожидание завершения генерации FakeTickServer (по MaxTicks)
# ============================================================
Write-Step "[6/7] Ожидание завершения генерации FakeTickServer"
# FakeServer НЕ завершает хост сам — только прекращает генерацию тиков.
# Ждём через polling /health с проверкой isLimitReached вместо blind sleep.
Wait-GenerationComplete -HealthUrl "http://localhost:5000/health" -MaxTicks $MaxTicks -Rps $Rps

# Статистика генерации: получаем финальные счётчики из /health
try {
    $genStats = Invoke-RestMethod -Uri "http://localhost:5000/health" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
    if ($genStats.uniqueTicks -ne $null) {
        $genTotal = [int]$genStats.totalTicks
        $genUnique = [int]$genStats.uniqueTicks
        $genDups = $genTotal - $genUnique
        $genDupPct = if ($genTotal -gt 0) { [Math]::Round($genDups / $genTotal * 100, 1) } else { 0 }
        Write-Host ""
        Write-Host "  Статистика генерации:" -ForegroundColor Cyan
        Write-Host "    Всего сгенерировано: $genTotal" -ForegroundColor Gray
        Write-Host "    Уникальных:          $genUnique" -ForegroundColor Green
        Write-Host "    Дублей (${genDupPct}%):  $genDups" -ForegroundColor Yellow
    }
}
catch {
    Write-Host "  Не удалось получить статистику генерации: $($_.Exception.Message)" -ForegroundColor Yellow
}

# ============================================================
# Сигнал Profiler'у: Phase 2 — дренаж + drained gcdump
# ============================================================
Write-Step "[6b/7] Сигнал Profiler'у на сбор drained gcdump"
if ($profilerProc -and (-not $profilerProc.HasExited)) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:5100/trigger-drained-collect" -Method Post -TimeoutSec 10
        Write-Host "  /trigger-drained-collect -> $($resp.status)" -ForegroundColor Green
    }
    catch {
        Write-Host "  Ошибка POST /trigger-drained-collect: $($_.Exception.Message)" -ForegroundColor Yellow
    }

    # Ждём завершения Profiler (Phase 2: drain + drained gcdump)
    Write-Host "  Ожидание Phase 2 (drain + drained gcdump)..." -ForegroundColor Yellow
    $profilerDeadline = (Get-Date).AddSeconds(180)
    $profilerExited = $false
    while ((Get-Date) -lt $profilerDeadline) {
        $profilerProc.Refresh()
        if ($profilerProc.HasExited) {
            Write-Host "  Profiler завершён (код $($profilerProc.ExitCode))." -ForegroundColor Green
            $profilerExited = $true
            break
        }
        Start-Sleep -Seconds 5
    }

    if (-not $profilerExited) {
        Write-Host "  Таймаут ожидания Profiler (180с) — принудительная остановка." -ForegroundColor Yellow
        taskkill /F /IM MarketDataCollector.Profiler.exe 2>$null
    }
}
else {
    # Если Profiler не запускался (SkipProfiler или ошибка сборки) — пауза для дренажа как раньше
    Write-Host "  Profiler не запущен — пауза для дренажа Worker (15с)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 15
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

# Статистика записи Worker: парсим итоговую строку из лога
$workerLogPath = Join-Path $root "$OutputDir/worker_out.log"
if (Test-Path $workerLogPath) {
    $workerFinalLine = Get-Content $workerLogPath | Select-String "Обработчик рыночных данных остановлен"
    if ($workerFinalLine) {
        $matchInserted = [regex]::Match($workerFinalLine, 'вставлено в БД:\s*(\d+)')
        $matchReceived = [regex]::Match($workerFinalLine, 'получено из канала:\s*(\d+)')
        $matchIncoming = [regex]::Match($workerFinalLine, 'Входящих:\s*(\d+)')
        $matchBacklog = [regex]::Match($workerFinalLine, 'backlog.*?:\s*(\d+)')
        if ($matchInserted.Success -and $matchReceived.Success) {
            $wrInserted = [int]$matchInserted.Groups[1].Value
            $wrReceived = [int]$matchReceived.Groups[1].Value
            $wrIncoming = if ($matchIncoming.Success) { [int]$matchIncoming.Groups[1].Value } else { 0 }
            $wrBacklog = if ($matchBacklog.Success) { [int]$matchBacklog.Groups[1].Value } else { $wrIncoming - $wrReceived }
            $wrDroppedPct = if ($wrIncoming -gt 0) { [Math]::Round($wrBacklog / $wrIncoming * 100, 1) } else { 0 }
            Write-Host ""
            Write-Host "  Статистика записи Worker:" -ForegroundColor Cyan
            Write-Host "    Входящих (Incoming):    $wrIncoming" -ForegroundColor Gray
            Write-Host "    Получено из канала:     $wrReceived" -ForegroundColor Gray
            Write-Host "    Дропнуто каналом:       $wrBacklog (${wrDroppedPct}%)" -ForegroundColor $(
                if ($wrDroppedPct -gt 10) { "Red" } else { "Yellow" })
            Write-Host "    Вставлено в БД:         $wrInserted" -ForegroundColor Green
        }
    }
}

# ============================================================
# Принудительная остановка FakeTickServer
# ============================================================
Write-Host "  Остановка FakeTickServer..." -ForegroundColor Yellow
taskkill /F /IM FakeTickServer.exe 2>$null
Start-Sleep -Seconds 1

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
