<#
.SYNOPSIS
    Сбор метрик и/или профилирование MarketDataCollector.Worker.

.DESCRIPTION
    Поддерживает несколько режимов работы через параметр -Mode:

    counters   — сбор Prometheus-метрик (бывшее поведение по умолчанию)
    trace      — dotnet-trace с allocation tracking (+ конвертация в SpeedScope)
    gcdump     — dotnet-gcdump (2 снапшота: на пике и после дренажа)
    all        — всё сразу: counters + trace + 2x gcdump

    Нажмите любую клавишу для остановки сбора метрик (или Ctrl+C).

.PARAMETER Mode
    Режим работы. Допустимые значения: counters, trace, gcdump, all. По умолчанию counters.

.PARAMETER MetricsUrl
    URL Prometheus metrics endpoint. По умолчанию http://localhost:5010/metrics.

.PARAMETER OutputDir
    Директория для результатов (CSV метрик / trace / gcdump). По умолчанию ./traces.

.PARAMETER RefreshSeconds
    Интервал опроса метрик в секундах (только для counters и all). По умолчанию 5.

.PARAMETER WorkerProcessName
    Имя процесса Worker для профилирования. По умолчанию MarketDataCollector.Worker.

.PARAMETER TraceDuration
    Длительность сбора dotnet-trace в секундах (trace/all). По умолчанию 60.

.PARAMETER GcDumpAtPeakSec
    Через сколько секунд от начала профилирования взять первый gcdump (на пике). По умолчанию 40.

.PARAMETER DrainWaitSec
    Сколько секунд ждать дренажа канала после завершения нагрузки перед вторым gcdump. По умолчанию 30.

.PARAMETER Duration
    Максимальная длительность сбора метрик в секундах (counters). 0 = без ограничений (ждать клавишу).

.PARAMETER Url
    [Устаревший синоним MetricsUrl] URL Prometheus metrics endpoint.

.EXAMPLE
    # Только сбор Prometheus-метрик (как было)
    .\run_counter.ps1 -Mode counters

    # Только dotnet-trace (60 сек)
    .\run_counter.ps1 -Mode trace -TraceDuration 60

    # Только gcdump: 2 снапшота (на пике через 40 сек и после дренажа)
    .\run_counter.ps1 -Mode gcdump -GcDumpAtPeakSec 40

    # Всё сразу (90 сек trace, gcdump на 50-й секунде)
    .\run_counter.ps1 -Mode all -TraceDuration 90 -GcDumpAtPeakSec 50

    # Кастомный процесс (например, dotnet если запущен через dotnet run)
    .\run_counter.ps1 -Mode all -WorkerProcessName "dotnet"
#>

param(
    [ValidateSet("counters", "trace", "gcdump", "all")]
    [string]$Mode = "counters",

    [string]$MetricsUrl = "http://localhost:5010/metrics",
    [string]$OutputDir = "./traces",
    [int]$RefreshSeconds = 5,
    [string]$WorkerProcessName = "MarketDataCollector.Worker",
    [int]$TraceDuration = 60,
    [int]$GcDumpAtPeakSec = 40,
    [int]$DrainWaitSec = 30,

    # Устаревшие/совместимость
    [int]$Duration = 0,
    [string]$Url = ""
)

$ErrorActionPreference = "Stop"

# Если указан устаревший параметр Url — используем его как MetricsUrl
if ($Url -and -not $MetricsUrl) {
    $MetricsUrl = $Url
}
# Если указан Duration (устаревший) — используем его для counters
if ($Duration -gt 0) {
    # ничего не делаем, Duration пока оставлен для обратной совместимости
}

# ============================================================
# Вспомогательные функции
# ============================================================

function Ensure-DotnetTools {
    <#
    .SYNOPSIS
        Проверяет наличие dotnet-trace и dotnet-gcdump, устанавливает при отсутствии.
    #>
    $tools = @("dotnet-trace", "dotnet-gcdump")
    $installed = @()
    $missing = @()

    Write-Host "[*] Проверка dotnet tools..." -ForegroundColor Cyan

    try {
        $globalList = dotnet tool list --global 2>&1
        foreach ($t in $tools) {
            if ($globalList -match [regex]::Escape($t)) {
                $installed += $t
            } else {
                $missing += $t
            }
        }
    } catch {
        Write-Host "[!] Не удалось получить список dotnet tools: $($_.Exception.Message)" -ForegroundColor Yellow
        # fallback: просто попробуем вызвать
        return
    }

    foreach ($m in $missing) {
        Write-Host "  → Устанавливаю $m..." -ForegroundColor Yellow
        dotnet tool install --global $m
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[!] Ошибка установки $m" -ForegroundColor Red
        } else {
            $installed += $m
        }
    }

    Write-Host "[+] Dotnet tools: $($installed -join ', ')" -ForegroundColor Green
}

function Find-ProcessId {
    <#
    .SYNOPSIS
        Находит PID процесса Worker по имени.
        Поддерживает: standalone exe, dotnet run, dotnet (dll).
    #>
    param([string]$ProcessName)

    Write-Host "[*] Поиск PID процесса '$ProcessName'..." -ForegroundColor Cyan

    # 1. Пробуем dotnet-trace ps (работает для .NET процессов)
    try {
        $tracePs = dotnet-trace ps 2>&1
        foreach ($line in $tracePs) {
            if ($line -match "^\s*(\d+)\s+$([regex]::Escape($ProcessName))") {
                $pid = [int]$Matches[1]
                Write-Host "[+] Найден PID: $pid (через dotnet-trace ps)" -ForegroundColor Green
                return $pid
            }
        }
    } catch {
        Write-Host "  dotnet-trace ps недоступен, пробую Get-Process..." -ForegroundColor DarkGray
    }

    # 2. Get-Process по имени (standalone exe)
    try {
        $proc = Get-Process -Name $ProcessName -ErrorAction SilentlyContinue
        if ($proc) {
            $pid = $proc.Id
            Write-Host "[+] Найден PID: $pid (через Get-Process)" -ForegroundColor Green
            return $pid
        }
    } catch {
        # Игнорируем
    }

    # 3. Поиск через Win32_Process по command line (dotnet run / dotnet <dll>)
    #    Ищем процесс dotnet.exe, в командной строке которого есть $ProcessName
    try {
        $procs = Get-CimInstance Win32_Process -Filter "Name='dotnet.exe'" -ErrorAction SilentlyContinue
        foreach ($p in $procs) {
            $cmdLine = $p.CommandLine
            if ($cmdLine -and $cmdLine -match [regex]::Escape($ProcessName)) {
                $pid = $p.ProcessId
                Write-Host "[+] Найден PID: $pid (dotnet run — '$ProcessName' в command line)" -ForegroundColor Green
                return $pid
            }
        }
    } catch {
        # Игнорируем
    }

    # 4. Последний шанс: ищем процесс с WindowTitle содержащим ProcessName
    try {
        $procs = Get-Process -ErrorAction SilentlyContinue | Where-Object {
            $_.MainWindowTitle -match [regex]::Escape($ProcessName)
        }
        if ($procs) {
            $pid = $procs[0].Id
            Write-Host "[+] Найден PID: $pid (через MainWindowTitle)" -ForegroundColor Green
            return $pid
        }
    } catch {
        # Игнорируем
    }

    Write-Host "[!] Процесс '$ProcessName' не найден." -ForegroundColor Red
    Write-Host "    Убедитесь, что Worker запущен." -ForegroundColor Yellow
    Write-Host "    Если запускаете через 'dotnet run', убедитесь, что окно терминала не свёрнуто." -ForegroundColor Yellow
    exit 1
}

function Start-TraceCollection {
    <#
    .SYNOPSIS
        Запускает dotnet-trace collect с профилем gc-verbose в фоновом режиме.
    .DESCRIPTION
        Возвращает PSCustomObject с Process и путём к выходному файлу.
    #>
    param(
        [int]$ProcessId,
        [int]$DurationSec = 60,
        [string]$OutputFilePath
    )

    $traceDir = Split-Path $OutputFilePath -Parent
    if (-not (Test-Path $traceDir)) {
        New-Item -ItemType Directory -Path $traceDir -Force | Out-Null
    }

    # Удаляем старый файл, если есть
    if (Test-Path $OutputFilePath) {
        Remove-Item $OutputFilePath -Force
    }

    Write-Host "[*] Запуск dotnet-trace collect (gc-verbose)..." -ForegroundColor Cyan
    Write-Host "    PID: $ProcessId"
    Write-Host "    Output: $OutputFilePath"
    Write-Host "    Duration: ${DurationSec}s"

    # dotnet-trace не поддерживает --duration напрямую, поэтому используем timeout
    # Запускаем через Start-Process в фоне и будем убивать по таймеру
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet-trace"
    $psi.Arguments = "collect --process-id $ProcessId --profile gc-verbose --output `"$OutputFilePath`""
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $proc = [System.Diagnostics.Process]::Start($psi)
    Write-Host "[+] dotnet-trace запущен (PID: $($proc.Id))" -ForegroundColor Green

    return [PSCustomObject]@{
        Process     = $proc
        OutputPath  = $OutputFilePath
        StartTime   = Get-Date
    }
}

function Stop-TraceCollection {
    <#
    .SYNOPSIS
        Останавливает dotnet-trace (Ctrl+C через SendCtrlC или Kill).
    #>
    param(
        [System.Diagnostics.Process]$TraceProcess,
        [int]$WaitSeconds = 15
    )

    if ($TraceProcess -eq $null -or $TraceProcess.HasExited) {
        Write-Host "[*] dotnet-trace уже завершён." -ForegroundColor DarkGray
        return
    }

    Write-Host "[*] Остановка dotnet-trace (PID: $($TraceProcess.Id))..." -ForegroundColor Yellow

    # Пытаемся优雅но завершить через Ctrl+C (только на Windows)
    try {
        # Отправляем Ctrl+C через GenerateConsoleCtrlEvent
        [Console]::TreatControlCAsInput = $false
        # Вызываем taskkill с Ctrl+C сигналом
        & taskkill /PID $TraceProcess.Id 2>$null
        # Ждём завершения
        $exited = $TraceProcess.WaitForExit($WaitSeconds * 1000)
        if ($exited) {
            Write-Host "[+] dotnet-trace остановлен." -ForegroundColor Green
            return
        }
    } catch {
        Write-Host "  Не удалось отправить Ctrl+C: $($_.Exception.Message)" -ForegroundColor DarkGray
    }

    # Force kill, если не завершился
    if (-not $TraceProcess.HasExited) {
        Write-Host "[!] Принудительное завершение dotnet-trace..." -ForegroundColor Yellow
        try {
            $TraceProcess.Kill()
            $TraceProcess.WaitForExit(5000)
            Write-Host "[-] dotnet-trace принудительно завершён." -ForegroundColor DarkGray
        } catch {
            Write-Host "[!] Не удалось завершить dotnet-trace: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

function Collect-GcDump {
    <#
    .SYNOPSIS
        Собирает дамп управляемой кучи через dotnet-gcdump.
    #>
    param(
        [int]$ProcessId,
        [string]$OutputPath,
        [string]$Label = ""
    )

    $dumpDir = Split-Path $OutputPath -Parent
    if (-not (Test-Path $dumpDir)) {
        New-Item -ItemType Directory -Path $dumpDir -Force | Out-Null
    }

    # Удаляем старый файл, если есть
    if (Test-Path $OutputPath) {
        Remove-Item $OutputPath -Force
    }

    Write-Host "[*] Сбор gcdump ($Label)..." -ForegroundColor Cyan
    Write-Host "    PID: $ProcessId"
    Write-Host "    Output: $OutputPath"

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet-gcdump"
    $psi.Arguments = "collect --process-id $ProcessId --output `"$OutputPath`""
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $proc = [System.Diagnostics.Process]::Start($psi)
    $proc.WaitForExit(60000)  # 60 sec timeout

    if ($proc.ExitCode -eq 0) {
        $fileSize = (Get-Item $OutputPath -ErrorAction SilentlyContinue).Length
        $sizeStr = if ($fileSize -gt 1MB) { "$([math]::Round($fileSize/1MB, 2)) MB" } else { "$([math]::Round($fileSize/1KB, 1)) KB" }
        Write-Host "[+] gcdump собран ($sizeStr)" -ForegroundColor Green
    } else {
        $stdErr = $proc.StandardError.ReadToEnd()
        Write-Host "[!] gcdump завершился с кодом $($proc.ExitCode)" -ForegroundColor Red
        Write-Host "    STDERR: $stdErr" -ForegroundColor Yellow
    }
}

function Convert-TraceToSpeedScope {
    <#
    .SYNOPSIS
        Конвертирует nettrace в формат SpeedScope (JSON) для визуализации.
    #>
    param([string]$TraceFile)

    if (-not (Test-Path $TraceFile)) {
        Write-Host "[!] Файл трассировки не найден: $TraceFile" -ForegroundColor Red
        return
    }

    $outputFile = [System.IO.Path]::ChangeExtension($TraceFile, ".speedscope.json")

    Write-Host "[*] Конвертация trace → SpeedScope..." -ForegroundColor Cyan
    Write-Host "    Input: $TraceFile"
    Write-Host "    Output: $outputFile"

    dotnet-trace convert --format speedscope "$TraceFile" --output "$outputFile" 2>&1
    if ($LASTEXITCODE -eq 0 -and (Test-Path $outputFile)) {
        $fileSize = (Get-Item $outputFile).Length
        $sizeStr = if ($fileSize -gt 1MB) { "$([math]::Round($fileSize/1MB, 2)) MB" } else { "$([math]::Round($fileSize/1KB, 1)) KB" }
        Write-Host "[+] SpeedScope файл создан ($sizeStr)" -ForegroundColor Green
        Write-Host "    Откройте в браузере: https://www.speedscope.app" -ForegroundColor DarkGray
    } else {
        Write-Host "[!] Ошибка конвертации trace" -ForegroundColor Red
    }
}

function WaitFor-PeakLoad {
    <#
    .SYNOPSIS
        Ожидание выхода на пик нагрузки (простая пауза).
    .DESCRIPTION
        Можно расширить для опроса /metrics endpoint'а для проверки backlog.
    #>
    param([int]$Seconds)

    Write-Host "[*] Ожидание пика нагрузки (${Seconds}s)..." -ForegroundColor Cyan

    for ($i = $Seconds; $i -gt 0; $i -= 5) {
        $remaining = [math]::Min($i, 5)
        Write-Host "    Осталось ${remaining}s..." -NoNewline
        Start-Sleep -Seconds $remaining
        Write-Host " ✓" -ForegroundColor DarkGray
    }
    Write-Host "[+] Пик нагрузки." -ForegroundColor Green
}

function WaitFor-Drain {
    <#
    .SYNOPSIS
        Ожидание дренажа канала (пауза + опционально проверка /metrics).
    #>
    param(
        [int]$TimeoutSec = 30,
        [string]$MetricsEndpoint = ""
    )

    Write-Host "[*] Ожидание дренажа канала (${TimeoutSec}s)..." -ForegroundColor Cyan

    # Если указан metrics endpoint — пробуем проверить backlog
    if ($MetricsEndpoint) {
        try {
            for ($i = 0; $i -lt $TimeoutSec; $i += 5) {
                $resp = Invoke-WebRequest -Uri $MetricsEndpoint -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
                if ($resp.Content -match 'processor_channel_backlog_count\s+(\d+)') {
                    $backlog = [int]$Matches[1]
                    Write-Host "    Backlog: $backlog"
                    if ($backlog -eq 0) {
                        Write-Host "[+] Канал дренирован." -ForegroundColor Green
                        return
                    }
                }
                Start-Sleep -Seconds 5
            }
        } catch {
            Write-Host "  Проверка /metrics недоступна: $($_.Exception.Message)" -ForegroundColor DarkGray
            # fallback: просто пауза
        }
    }

    # Fallback: простая пауза
    for ($i = $TimeoutSec; $i -gt 0; $i -= 5) {
        $remaining = [math]::Min($i, 5)
        Write-Host "    Осталось ${remaining}s..." -NoNewline
        Start-Sleep -Seconds $remaining
        Write-Host " ✓" -ForegroundColor DarkGray
    }
    Write-Host "[+] Ожидание завершено." -ForegroundColor Green
}

function Write-SectionHeader {
    param([string]$Title)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host ""
}

# ============================================================
# Парсинг Prometheus text format → CSV
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
# Режим: сбор Prometheus-метрик (counters)
# ============================================================
function Invoke-CountersCollection {
    param(
        [string]$Url,
        [string]$OutputDir,
        [int]$RefreshSeconds,
        [int]$Duration = 0
    )

    Write-SectionHeader "Режим: сбор Prometheus-метрик"

    # ============================================================
    # 1. Проверяем доступность Worker
    # ============================================================
    Write-Host "[*] Checking Worker at $Url ..." -ForegroundColor Cyan
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
        if ($response.StatusCode -ne 200) {
            Write-Host "[!] Worker returned HTTP $($response.StatusCode)" -ForegroundColor Red
            return
        }
        $metricCount = ($response.Content -split "`n" | Where-Object { $_ -match "^[a-z_]" }).Count
        Write-Host "[+] Worker is alive. Found $metricCount metric entries in Prometheus format." -ForegroundColor Green
    } catch {
        Write-Host "[!] Cannot reach Worker at $Url" -ForegroundColor Red
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
}

# ============================================================
# Режим: dotnet-trace (trace)
# ============================================================
function Invoke-TraceCollection {
    param(
        [string]$ProcessName,
        [int]$DurationSec,
        [string]$OutputDir
    )

    Write-SectionHeader "Режим: dotnet-trace (allocation tracking)"

    $traceFile = Join-Path (Resolve-Path $OutputDir) "allocation_trace_$(Get-Date -Format 'yyyyMMdd_HHmmss').nettrace"

    # Найти PID
    $pid = Find-ProcessId -ProcessName $ProcessName

    # Запустить trace
    $traceJob = Start-TraceCollection -ProcessId $pid -DurationSec $DurationSec -OutputFilePath $traceFile

    # Ждать нужное время
    Write-Host "[*] Сбор allocation trace в течение ${DurationSec}с..." -ForegroundColor Cyan
    $elapsed = 0
    while ($elapsed -lt $DurationSec) {
        if ($traceJob.Process.HasExited) {
            Write-Host "`n[!] dotnet-trace завершился раньше времени." -ForegroundColor Yellow
            break
        }
        Start-Sleep -Seconds 1
        $elapsed++
        if ($elapsed % 10 -eq 0) {
            Write-Host "    Прошло ${elapsed}s / ${DurationSec}s..."
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
}

# ============================================================
# Режим: dotnet-gcdump (gcdump)
# ============================================================
function Invoke-GcDumpCollection {
    param(
        [string]$ProcessName,
        [int]$PeakAfterSec,
        [int]$DrainWaitSec,
        [string]$OutputDir,
        [string]$MetricsEndpoint = ""
    )

    Write-SectionHeader "Режим: dotnet-gcdump (2 снапшота)"

    $pid = Find-ProcessId -ProcessName $ProcessName

    $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $peakFile = Join-Path (Resolve-Path $OutputDir) "snapshot_peak_$timestamp.gcdump"
    $drainedFile = Join-Path (Resolve-Path $OutputDir) "snapshot_drained_$timestamp.gcdump"

    # Снапшот #1: на пике нагрузки
    WaitFor-PeakLoad -Seconds $PeakAfterSec
    Collect-GcDump -ProcessId $pid -OutputPath $peakFile -Label "PEAK"

    # Ожидание дренажа
    WaitFor-Drain -TimeoutSec $DrainWaitSec -MetricsEndpoint $MetricsEndpoint

    # Снапшот #2: после стабилизации
    Collect-GcDump -ProcessId $pid -OutputPath $drainedFile -Label "DRAINED"

    Write-Host ""
    Write-Host "[+] Результаты gcdump:" -ForegroundColor Green
    Write-Host "    Peak:    $peakFile" -ForegroundColor DarkGray
    Write-Host "    Drained: $drainedFile" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "    Для анализа откройте файлы в Visual Studio:" -ForegroundColor DarkGray
    Write-Host "    Debug → Memory Usage → Load Snapshot" -ForegroundColor DarkGray
}

# ============================================================
# Режим: всё сразу (all)
# ============================================================
function Invoke-AllInOne {
    param(
        [string]$ProcessName,
        [string]$MetricsUrl,
        [string]$OutputDir,
        [int]$RefreshSeconds,
        [int]$TraceDuration,
        [int]$GcDumpAtPeakSec,
        [int]$DrainWaitSec
    )

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

    $pid = Find-ProcessId -ProcessName $ProcessName

    Write-Host "[*] Параметры запуска:" -ForegroundColor Cyan
    Write-Host "    Worker PID:       $pid"
    Write-Host "    Trace duration:   ${TraceDuration}s"
    Write-Host "    GcDump at peak:   ${GcDumpAtPeakSec}s"
    Write-Host "    Drain wait:       ${DrainWaitSec}s"
    Write-Host "    Output dir:       $resolveDir"
    Write-Host ""

    # ============================================================
    # 1. Запуск dotnet-trace в фоне
    # ============================================================
    $traceJob = Start-TraceCollection -ProcessId $pid -DurationSec $TraceDuration -OutputFilePath $traceFile

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

        try {
            $null = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
        } catch {
            Write-Error "Worker недоступен"
            return
        }
        "Timestamp,Metric,Labels,Type,Value,Description" | Out-File -FilePath $file -Encoding UTF8

        $startTime = Get-Date
        $sampleCount = 0
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
            } catch { }
            Start-Sleep -Seconds $interval
        }
    } -ArgumentList $MetricsUrl, $csvFile, $RefreshSeconds

    Write-Host "[*] Сбор метрик запущен в фоне (Job ID: $($countersJob.Id))" -ForegroundColor DarkGray

    # ============================================================
    # 3. Ожидание пика нагрузки → gcdump #1
    # ============================================================
    WaitFor-PeakLoad -Seconds $GcDumpAtPeakSec
    Collect-GcDump -ProcessId $pid -OutputPath $peakFile -Label "PEAK"

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
            if ($elapsed % 10 -eq 0) { Write-Host "    Осталось $($remaining - $elapsed)s..." }
        }
    }

    # ============================================================
    # 5. Остановка trace
    # ============================================================
    Stop-TraceCollection -TraceProcess $traceJob.Process

    # ============================================================
    # 6. Ожидание дренажа → gcdump #2
    # ============================================================
    WaitFor-Drain -TimeoutSec $DrainWaitSec -MetricsEndpoint $MetricsUrl
    Collect-GcDump -ProcessId $pid -OutputPath $drainedFile -Label "DRAINED"

    # ============================================================
    # 7. Конвертация trace → SpeedScope
    # ============================================================
    if (Test-Path $traceFile) {
        Convert-TraceToSpeedScope -TraceFile $traceFile
    }

    # ============================================================
    # 8. Остановка counters job
    # ============================================================
    Write-Host "[*] Остановка сбора метрик..." -ForegroundColor DarkGray
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
}

# ============================================================
# MAIN
# ============================================================

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║      MarketDataCollector — Профилирование               ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Убедиться, что dotnet tools установлены
Ensure-DotnetTools

# Создать выходную директорию
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

# Выбор режима
switch ($Mode) {
    "counters" {
        Invoke-CountersCollection -Url $MetricsUrl -OutputDir $OutputDir `
            -RefreshSeconds $RefreshSeconds -Duration $Duration
    }
    "trace" {
        Invoke-TraceCollection -ProcessName $WorkerProcessName `
            -DurationSec $TraceDuration -OutputDir $OutputDir
    }
    "gcdump" {
        Invoke-GcDumpCollection -ProcessName $WorkerProcessName `
            -PeakAfterSec $GcDumpAtPeakSec -DrainWaitSec $DrainWaitSec `
            -OutputDir $OutputDir -MetricsEndpoint $MetricsUrl
    }
    "all" {
        Invoke-AllInOne -ProcessName $WorkerProcessName -MetricsUrl $MetricsUrl `
            -OutputDir $OutputDir -RefreshSeconds $RefreshSeconds `
            -TraceDuration $TraceDuration -GcDumpAtPeakSec $GcDumpAtPeakSec `
            -DrainWaitSec $DrainWaitSec
    }
}

Write-Host ""
Write-Host "[√] Готово." -ForegroundColor Green
