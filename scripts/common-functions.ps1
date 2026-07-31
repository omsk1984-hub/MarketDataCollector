<#
.SYNOPSIS
    Общие функции для скриптов сбора метрик MarketDataCollector.
.DESCRIPTION
    Содержит вспомогательные функции, используемые всеми скриптами сбора метрик:
    - Ensure-DotnetTools
    - Find-ProcessId
    - Start-TraceCollection
    - Stop-TraceCollection
    - Collect-GcDump
    - Convert-TraceToSpeedScope
    - WaitFor-PeakLoad
    - WaitFor-Drain
    - Write-SectionHeader
    - Parse-PrometheusMetrics
    - Show-CountersJobProgress
#>

$ErrorActionPreference = "Stop"

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

    # 1. Пробуем dotnet-trace ps
    try {
        $traceOutput = & { dotnet-trace ps 2>&1 | Out-String }
        foreach ($line in ($traceOutput -split "`n")) {
            if ($line -match "^\s*(\d+)\s+$([regex]::Escape($ProcessName))") {
                $foundPid = [int]$Matches[1]
                Write-Host "[+] Найден PID: $foundPid (через dotnet-trace ps)" -ForegroundColor Green
                return $foundPid
            }
        }
        Write-Host "  dotnet-trace ps: процесс '$ProcessName' не в списке, пробую другие методы..." -ForegroundColor DarkGray
    } catch {
        Write-Host "  dotnet-trace ps недоступен: $($_.Exception.Message)" -ForegroundColor DarkGray
    }

    # 2. Get-Process по имени
    try {
        $proc = Get-Process -Name $ProcessName -ErrorAction SilentlyContinue
        if ($proc) {
            $foundPid = $proc.Id
            Write-Host "[+] Найден PID: $foundPid (через Get-Process)" -ForegroundColor Green
            return $foundPid
        }
    } catch {
        # Игнорируем
    }

    # 3. Поиск через Win32_Process
    try {
        $dotnetProcs = Get-CimInstance Win32_Process -Filter "Name='dotnet.exe'" -ErrorAction SilentlyContinue
        foreach ($p in $dotnetProcs) {
            $cmdLine = $p.CommandLine
            if (-not $cmdLine) { continue }

            if ($cmdLine -match [regex]::Escape($ProcessName)) {
                $foundPid = $p.ProcessId
                Write-Host "[+] Найден PID: $foundPid (Win32_Process — '$ProcessName' в command line)" -ForegroundColor Green
                return $foundPid
            }

            if ($cmdLine -match 'dotnet\.exe"\s+run\s*$') {
                try {
                    $procObj = Get-Process -Id $p.ProcessId -ErrorAction SilentlyContinue
                    if ($procObj) {
                        $modules = $procObj.Modules | ForEach-Object { $_.ModuleName }
                        foreach ($mod in $modules) {
                            if ($mod -match [regex]::Escape($ProcessName)) {
                                $foundPid = $p.ProcessId
                                Write-Host "[+] Найден PID: $foundPid (dotnet run — модуль '$mod')" -ForegroundColor Green
                                return $foundPid
                            }
                        }
                    }
                } catch {
                    # Игнорируем ошибки доступа к модулям
                }
            }
        }
    } catch {
        # Игнорируем
    }

    # 4. Последний шанс: MainWindowTitle
    try {
        $procs = Get-Process -ErrorAction SilentlyContinue | Where-Object {
            $_.MainWindowTitle -match [regex]::Escape($ProcessName)
        }
        if ($procs) {
            $foundPid = $procs[0].Id
            Write-Host "[+] Найден PID: $foundPid (через MainWindowTitle)" -ForegroundColor Green
            return $foundPid
        }
    } catch {
        # Игнорируем
    }

    Write-Host "[!] Процесс '$ProcessName' не найден ни одним из методов:" -ForegroundColor Red
    Write-Host "    1) dotnet-trace ps" -ForegroundColor DarkGray
    Write-Host "    2) Get-Process -Name '$ProcessName'" -ForegroundColor DarkGray
    Write-Host "    3) Win32_Process (command line / modules)" -ForegroundColor DarkGray
    Write-Host "    4) MainWindowTitle" -ForegroundColor DarkGray
    Write-Host "" -ForegroundColor Yellow
    Write-Host "    Убедитесь, что Worker запущен." -ForegroundColor Yellow
    Write-Host "    Если запускаете через 'dotnet run', Worker должен работать в отдельном терминале." -ForegroundColor Yellow
    Write-Host "    Также попробуйте: .\metrics.ps1 -Mode all -WorkerProcessName 'dotnet'" -ForegroundColor Yellow
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

    if (Test-Path $OutputFilePath) {
        Remove-Item $OutputFilePath -Force
    }

    Write-Host "[*] Запуск dotnet-trace collect (gc-verbose)..." -ForegroundColor Cyan
    Write-Host "    PID: $ProcessId"
    Write-Host "    Output: $OutputFilePath"
    Write-Host "    Duration: ${DurationSec}s"

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet-trace"
    $psi.Arguments = "collect --process-id $ProcessId --profile gc-verbose --duration $DurationSec --output `"$OutputFilePath`""
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $proc = [System.Diagnostics.Process]::Start($psi)

    # ВАЖНО: перенаправленный stdout/stderr нужно читать, иначе pipe-буфер переполнится
    # и dotnet-trace заблокируется на записи (deadlock). Запускаем асинхронное чтение
    # fire-and-forget и храним задачи в объекте, чтобы они не были собраны GC.
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()

    Write-Host "[+] dotnet-trace запущен (PID: $($proc.Id))" -ForegroundColor Green

    return [PSCustomObject]@{
        Process     = $proc
        OutputPath  = $OutputFilePath
        StartTime   = Get-Date
        StdoutTask  = $stdoutTask
        StderrTask  = $stderrTask
    }
}

function Stop-TraceCollection {
    <#
    .SYNOPSIS
        Останавливает dotnet-trace (WaitForExit → taskkill → Kill).
    .DESCRIPTION
        dotnet-trace запускается с --duration и завершается сам, финализируя .nettrace.
        Поэтому приоритет — дождаться естественного завершения (WaitForExit).
        Фолбэки taskkill/Kill — только страховка на случай зависания.

        ВАЖНО: здесь НЕ используется AttachConsole/FreeConsole/GenerateConsoleCtrlEvent.
        Прикрепление PowerShell к консоли дочернего процесса без последующего
        FreeConsole оставляет pwsh привязанным к умирающей консоли dotnet-trace,
        что вызывает CLR crash процесса pwsh с кодом 0xE0434352.
    #>
    param(
        [System.Diagnostics.Process]$TraceProcess,
        [int]$WaitSeconds = 15
    )

    if ($TraceProcess -eq $null -or $TraceProcess.HasExited) {
        Write-Host "[*] dotnet-trace уже завершён." -ForegroundColor DarkGray
        return
    }

    Write-Host "[*] Ожидание завершения dotnet-trace (PID: $($TraceProcess.Id))..." -ForegroundColor Yellow

    # 1. Ждём естественного завершения (dotnet-trace с --duration завершится сам и финализирует .nettrace)
    if ($TraceProcess.WaitForExit($WaitSeconds * 1000)) {
        Write-Host "[+] dotnet-trace остановлен (завершился по --duration)." -ForegroundColor Green
        return
    }

    # 2. Фолбэк: taskkill без /F (более мягкий)
    Write-Host "[!] dotnet-trace не завершился за ${WaitSeconds}s, пробую taskkill..." -ForegroundColor Yellow
    try {
        & taskkill /PID $TraceProcess.Id 2>$null | Out-Null
    } catch { }
    if ($TraceProcess.WaitForExit(5000)) {
        Write-Host "[+] dotnet-trace остановлен." -ForegroundColor Green
        return
    }

    # 3. Последний фолбэк
    Write-Host "[!] Принудительное завершение dotnet-trace..." -ForegroundColor Yellow
    try {
        $TraceProcess.Kill()
        $TraceProcess.WaitForExit(5000)
        Write-Host "[-] dotnet-trace принудительно завершён." -ForegroundColor DarkGray
    } catch {
        Write-Host "[!] Не удалось завершить dotnet-trace: $($_.Exception.Message)" -ForegroundColor Red
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

    # ВАЖНО: не используем event-based async чтение (add_OutputDataReceived / add_ErrorDataReceived
    # со scriptblock'ами). При большом потоке вывода (dotnet-gcdump при длительном сборе пишет много)
    # event-callback'и выполняются в разных контекстах и вызывают гонку -> необработанное исключение
    # в CLR -> краш самого pwsh с кодом 0xE0434352 (-532462766).
    # Вместо этого читаем stdout/stderr асинхронно через ReadToEndAsync (не блокирует процесс).
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()

    # Ждём завершения (до 120 сек). При зависании — принудительно завершаем.
    if (-not $proc.WaitForExit(120000)) {
        Write-Host "[!] gcdump не завершился за 120s, принудительное завершение..." -ForegroundColor Yellow
        try { $proc.Kill() } catch { }
        $proc.WaitForExit(5000)
    }

    $stdout = ""
    $stderr = ""
    try { $stdout = $stdoutTask.GetAwaiter().GetResult() } catch { }
    try { $stderr = $stderrTask.GetAwaiter().GetResult() } catch { }

    if ($proc.ExitCode -eq 0) {
        $fileSize = (Get-Item $OutputPath -ErrorAction SilentlyContinue).Length
        $sizeStr = if ($fileSize -gt 1MB) { "$([math]::Round($fileSize/1MB, 2)) MB" } else { "$([math]::Round($fileSize/1KB, 1)) KB" }
        Write-Host "[+] gcdump собран ($sizeStr)" -ForegroundColor Green
    } else {
        $stdErr = $stderr.Trim()
        if (-not $stdErr) { $stdErr = $stdout.Trim() }
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

    $convertOut = dotnet-trace convert --format speedscope "$TraceFile" --output "$outputFile" 2>&1
    $convertText = ($convertOut | Out-String)
    if ($convertText) {
        Write-Host $convertText.Trim()
    }
    if ($LASTEXITCODE -eq 0 -and (Test-Path $outputFile)) {
        $fileSize = (Get-Item $outputFile).Length
        $sizeStr = if ($fileSize -gt 1MB) { "$([math]::Round($fileSize/1MB, 2)) MB" } else { "$([math]::Round($fileSize/1KB, 1)) KB" }
        Write-Host "[+] SpeedScope файл создан ($sizeStr)" -ForegroundColor Green
        Write-Host "    Откройте в браузере: https://www.speedscope.app" -ForegroundColor DarkGray
    } else {
        Write-Host "[!] Ошибка конвертации trace" -ForegroundColor Red
        if ($convertText -match 'broken|best-effort') {
            Write-Host "    Trace-файл, похоже, битый (не финализирован). Стеки будут неполными." -ForegroundColor Yellow
            Write-Host "    Причина: dotnet-trace остановлен принудительно (taskkill/Kill) вместо Ctrl+C." -ForegroundColor Yellow
        }
    }
}

function WaitFor-PeakLoad {
    <#
    .SYNOPSIS
        Ожидание выхода на пик нагрузки (простая пауза).
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
    .DESCRIPTION
        Использует единственный таймер TimeoutSec. Если /metrics недоступен,
        переключается на простой countdown с предупреждением.
    #>
    param(
        [int]$TimeoutSec = 30,
        [string]$MetricsEndpoint = ""
    )

    Write-Host "[*] Ожидание дренажа канала (${TimeoutSec}s)..." -ForegroundColor Cyan

    $useMetrics = -not [string]::IsNullOrWhiteSpace($MetricsEndpoint)
    $metricsFailed = $false
    $startTime = Get-Date

    while ($true) {
        $elapsed = ((Get-Date) - $startTime).TotalSeconds
        if ($elapsed -ge $TimeoutSec) { break }

        $remaining = $TimeoutSec - $elapsed

        if ($useMetrics -and -not $metricsFailed) {
            try {
                $resp = Invoke-WebRequest -Uri $MetricsEndpoint -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
                if ($resp.Content -match 'processor_channel_backlog_count\s+(\d+)') {
                    $backlog = [int]$Matches[1]
                    Write-Host "    Backlog: $backlog  (${([math]::Round($remaining,0))}s left)"
                    if ($backlog -eq 0) {
                        Write-Host "[+] Канал дренирован." -ForegroundColor Green
                        return
                    }
                } else {
                    Write-Host "    /metrics доступен, но backlog не найден  (${([math]::Round($remaining,0))}s left)" -ForegroundColor DarkGray
                }
            } catch {
                $metricsFailed = $true
                Write-Host "  /metrics недоступен, переключаюсь на таймер: $($_.Exception.Message)" -ForegroundColor DarkGray
            }
        } else {
            # Fallback: простой countdown
            $chunk = [math]::Min(5, [math]::Ceiling($remaining))
            Write-Host "    Осталось ${([math]::Round($remaining,0))}s..." -NoNewline
            Start-Sleep -Seconds $chunk
            Write-Host " ✓" -ForegroundColor DarkGray
            continue
        }

        Start-Sleep -Seconds 5
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

function Show-CountersJobProgress {
    <#
    .SYNOPSIS
        Выводит в консоль накопленный вывод из background job сбора счётчиков.
    #>
    param(
        [System.Management.Automation.Job]$Job
    )
    if ($Job -eq $null) { return }
    try {
        $output = Receive-Job -Job $Job -ErrorAction SilentlyContinue
        if ($output) {
            foreach ($line in $output) {
                if ($line -match '\[COUNTERS\]') {
                    Write-Host "    $line" -ForegroundColor DarkCyan
                }
            }
        }
    } catch {
        # Игнорируем ошибки чтения job output
    }
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
