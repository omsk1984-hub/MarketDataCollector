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
        [string]$OutputFilePath,
        [ValidateSet("gc-verbose", "cpu-sampling", "contention", "contention-cpu")]
        [string]$Profile = "gc-verbose"
    )

    $traceDir = Split-Path $OutputFilePath -Parent
    if (-not (Test-Path $traceDir)) {
        New-Item -ItemType Directory -Path $traceDir -Force | Out-Null
    }

    if (Test-Path $OutputFilePath) {
        Remove-Item $OutputFilePath -Force
    }

    # Профиль сбора определяет набор провайдеров/ключевых слов dotnet-trace.
    #   - gc-verbose      : аллокации/GC (по умолчанию, прошлое поведение)
    #   - cpu-sampling    : CPU-стеки (topN)
    #   - contention      : ТОЛЬКО contention-события (0x4000) — report topN не даст
    #                       CPU-стеков («No method calls found»), пригодно для количественной
    #                       проверки contention через событие Contention/ContentionStop.
    #   - contention-cpu  : contention + CPU-sampling ОДНОВРЕМЕННО. Собираются и
    #                       contention-события (0x4000), и CPU-сэмплы (SampleProfiler),
    #                       поэтому dotnet-trace report topN даёт стеки, а по событию
    #                       Contention можно локализовать, в каких методах происходит
    #                       конкуренция за lock (требование Этапа 0 плана).
    $traceArgs = switch ($Profile.ToLowerInvariant()) {
        "cpu-sampling"    { "--profile cpu-sampling" }
        "contention"      { "--providers Microsoft-Windows-DotNETRuntime:0x4000:5" }
        "contention-cpu"  { "--providers Microsoft-Windows-DotNETRuntime:0x4000:5,Microsoft-DotNETCore-SampleProfiler:0:5" }
        default           { "--profile gc-verbose" }
    }

    Write-Host "[*] Запуск dotnet-trace collect (profile: $Profile)..." -ForegroundColor Cyan
    Write-Host "    PID: $ProcessId"
    Write-Host "    Output: $OutputFilePath"
    Write-Host "    Duration: ${DurationSec}s"
    Write-Host "    Args: $traceArgs"

    # dotnet-trace интерпретирует голое число в --duration как ДНИ (TimeSpan default),
    # а не секунды. Это приводило к System.ArgumentException 'Invalid value ... for
    # parameter interval' (System.Timers.Timer не принимает интервал > Int32.MaxValue мс).
    # Поэтому передаём длительность в явном формате hh:mm:ss.
    $hours = [math]::Floor($DurationSec / 3600)
    $minutes = [math]::Floor(($DurationSec % 3600) / 60)
    $seconds = $DurationSec % 60
    $durationArg = "{0:00}:{1:00}:{2:00}" -f $hours, $minutes, $seconds
    Write-Host "    Duration (hh:mm:ss): $durationArg"

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet-trace"
    $psi.Arguments = "collect --process-id $ProcessId $traceArgs --duration $durationArg --output `"$OutputFilePath`""
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
        Останавливает dotnet-trace (WaitForExit → taskkill → Kill) с диагностикой ExitCode.
    .DESCRIPTION
        dotnet-trace запускается с --duration и завершается сам, финализируя .nettrace.
        Поэтому приоритет — дождаться естественного завершения (WaitForExit).
        Фолбэки taskkill/Kill — только страховка на случай зависания.

        Параметр -TraceJob ожидает объект, возвращённый Start-TraceCollection
        (поля Process, OutputPath, StdoutTask, StderrTask). По завершении процесса
        читаются ExitCode и stdout/stderr, что позволяет диагностировать молчаливые
        сбои (отказ attach, конфликт профиля, сбой финализации .nettrace).

        ВАЖНО: здесь НЕ используется AttachConsole/FreeConsole/GenerateConsoleCtrlEvent.
        Прикрепление PowerShell к консоли дочернего процесса без последующего
        FreeConsole оставляет pwsh привязанным к умирающей консоли dotnet-trace,
        что вызывает CLR crash процесса pwsh с кодом 0xE0434352.
    #>
    param(
        [System.Diagnostics.Process]$TraceProcess,
        [int]$WaitSeconds = 15,
        [PSObject]$TraceJob = $null
    )

    if ($TraceProcess -eq $null) {
        Write-Host "[!] dotnet-trace: объект Process не передан." -ForegroundColor Red
        return
    }

    $exitedNaturally = $TraceProcess.HasExited
    if (-not $exitedNaturally) {
        Write-Host "[*] Ожидание завершения dotnet-trace (PID: $($TraceProcess.Id))..." -ForegroundColor Yellow

        # 1. Ждём естественного завершения (dotnet-trace с --duration завершится сам и финализирует .nettrace)
        if ($TraceProcess.WaitForExit($WaitSeconds * 1000)) {
            $exitedNaturally = $true
            Write-Host "[+] dotnet-trace остановлен (завершился по --duration)." -ForegroundColor Green
        } else {
            # 2. Фолбэк: taskkill без /F (более мягкий)
            Write-Host "[!] dotnet-trace не завершился за ${WaitSeconds}s, пробую taskkill..." -ForegroundColor Yellow
            try {
                & taskkill /PID $TraceProcess.Id 2>$null | Out-Null
            } catch { }
            if ($TraceProcess.WaitForExit(5000)) {
                Write-Host "[+] dotnet-trace остановлен." -ForegroundColor Green
            } else {
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
        }
    } else {
        Write-Host "[*] dotnet-trace уже завершён." -ForegroundColor DarkGray
    }

    # Диагностика результата: ExitCode + stdout/stderr + наличие файла
    $stdout = ""
    $stderr = ""
    if ($TraceJob -ne $null) {
        try { $stdout = $TraceJob.StdoutTask.GetAwaiter().GetResult() } catch { }
        try { $stderr = $TraceJob.StderrTask.GetAwaiter().GetResult() } catch { }
    }

    $exitCode = $null
    try { $exitCode = $TraceProcess.ExitCode } catch { }

    $tracePath = if ($TraceJob -ne $null -and $TraceJob.OutputPath) { $TraceJob.OutputPath } else { "" }
    $fileOk = (-not [string]::IsNullOrWhiteSpace($tracePath)) -and (Test-Path $tracePath) -and ((Get-Item $tracePath).Length -gt 0)

    if ($exitCode -ne $null -and $exitCode -ne 0) {
        Write-Host "[!] dotnet-trace завершился с кодом $exitCode" -ForegroundColor Red
        $errText = $stderr.Trim()
        if (-not $errText) { $errText = $stdout.Trim() }
        if ($errText) { Write-Host "    STDERR: $errText" -ForegroundColor Yellow }
    } elseif (-not $fileOk) {
        Write-Host "[!] dotnet-trace завершился, но файл трассировки не найден/пуст: $tracePath" -ForegroundColor Red
        $errText = $stderr.Trim()
        if (-not $errText) { $errText = $stdout.Trim() }
        if ($errText) { Write-Host "    Вывод: $errText" -ForegroundColor Yellow }
    } else {
        Write-Host "[+] dotnet-trace: ExitCode=$exitCode, nettrace готов: $tracePath" -ForegroundColor Green
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

    # ===== ВАЛИДАЦИЯ (только диагностика, без изменения поведения) =====
    # 1) Проверка живости целевого процесса перед attach.
    $targetAlive = $false
    $targetObj = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($targetObj) {
        $targetAlive = $true
        Write-Host "    [DIAG] PID $ProcessId жив (WS=$([math]::Round($targetObj.WorkingSet64/1MB,1)) MB, Responding=$($targetObj.Responding))" -ForegroundColor DarkGray
    } else {
        Write-Host "    [DIAG] ВНИМАНИЕ: PID $ProcessId НЕ существует на момент сборки gcdump ($Label)" -ForegroundColor Yellow
    }

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "dotnet-gcdump"
    $psi.Arguments = "collect --process-id $ProcessId --output `"$OutputPath`""
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $proc = [System.Diagnostics.Process]::Start($psi)
    Write-Host "    [DIAG] dotnet-gcdump запущен (PID: $($proc.Id), t=$([math]::Round((Get-Date -DisplayHint Time).TimeOfDay.TotalSeconds))s)" -ForegroundColor DarkGray
    [Console]::Out.Flush()

    # ВАЖНО: не используем event-based async чтение (add_OutputDataReceived / add_ErrorDataReceived
    # со scriptblock'ами). При большом потоке вывода (dotnet-gcdump при длительном сбое пишет много)
    # event-callback'и выполняются в разных контекстах и вызывают гонку -> необработанное исключение
    # в CLR -> краш самого pwsh с кодом 0xE0434352 (-532462766).
    # Вместо этого читаем stdout/stderr асинхронно через ReadToEndAsync (не блокирует процесс).
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()
    Write-Host "    [DIAG] ReadToEndAsync стартовал, начинаю WaitForExit..." -ForegroundColor DarkGray
    [Console]::Out.Flush()

    # Ждём завершения (до 120 сек). При зависании — принудительно завершаем.
    $waitStart = Get-Date
    $waitResult = $proc.WaitForExit(120000)
    Write-Host "    [DIAG] DEBUG waitStart type=$($waitStart.GetType().Name) waitResult type=$($waitResult.GetType().Name)" -ForegroundColor DarkGray
    Write-Host "    [DIAG] WaitForExit вернул $waitResult (t=$([math]::Round(((Get-Date) - $waitStart).TotalSeconds))s)" -ForegroundColor DarkGray
    [Console]::Out.Flush()
    if (-not $waitResult) {
        Write-Host "[!] gcdump не завершился за 120s, принудительное завершение..." -ForegroundColor Yellow
        try { $proc.Kill() } catch { }
        $proc.WaitForExit(5000)
    }

    # После Kill чтение задач может блокироваться, если pipe-буфер не закрыт процессом.
    # Здесь только засекаем время выполнения чтения, чтобы выявить зависание.
    $readStart = Get-Date
    $stdout = ""
    $stderr = ""
    try { $stdout = $stdoutTask.GetAwaiter().GetResult() } catch { Write-Host "    [DIAG] Ошибка чтения stdout: $($_.Exception.Message)" -ForegroundColor Yellow }
    try { $stderr = $stderrTask.GetAwaiter().GetResult() } catch { Write-Host "    [DIAG] Ошибка чтения stderr: $($_.Exception.Message)" -ForegroundColor Yellow }
    Write-Host "    [DIAG] Чтение stdout/stderr заняло t=$([math]::Round(((Get-Date) - $readStart).TotalSeconds))s" -ForegroundColor DarkGray
    Write-Host "    [DIAG] ExitCode=$($proc.ExitCode), файл существует=$(Test-Path $OutputPath)" -ForegroundColor DarkGray
    # ===== КОНЕЦ ВАЛИДАЦИИ =====

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
    # dotnet-trace convert --format speedscope сам добавляет суффикс ".speedscope.json"
    # к имени из --output. Если передать полный путь с расширением, получится задвоенный
    # суффикс (....speedscope.speedscope.json). Поэтому передаём базовое имя без расширения.
    $outputBase = [System.IO.Path]::ChangeExtension($TraceFile, "")

    Write-Host "[*] Конвертация trace → SpeedScope..." -ForegroundColor Cyan
    Write-Host "    Input: $TraceFile"
    Write-Host "    Output: $outputFile"

    $convertOut = dotnet-trace convert --format speedscope "$TraceFile" --output "$outputBase" 2>&1
    $convertText = ($convertOut | Out-String)
    if ($convertText) {
        Write-Host $convertText.Trim()
    }

    # Нормализация: если dotnet-trace создал файл с задвоенным суффиксом — переименовываем
    # в ожидаемое имя, чтобы последующие проверки в отчёте совпадали.
    $actualFile = $outputFile
    if (-not (Test-Path -LiteralPath $actualFile)) {
        $fileNameBase = [System.IO.Path]::GetFileNameWithoutExtension($TraceFile)
        $candidates = @(Get-ChildItem -Path (Split-Path $TraceFile) -Filter "*.speedscope.json" -File -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like "$fileNameBase*" })
        if ($candidates.Count -gt 0) {
            $actualFile = $candidates[0].FullName
            if ($actualFile -ne $outputFile) {
                Move-Item -LiteralPath $actualFile -Destination $outputFile -Force
                $actualFile = $outputFile
            }
        }
    }

    if ($LASTEXITCODE -eq 0 -and (Test-Path -LiteralPath $actualFile)) {
        $fileSize = (Get-Item -LiteralPath $actualFile).Length
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
                # Prometheus format: метрика может содержать лейблы {...} между именем и значением.
                if ($resp.Content -match 'processor_channel_backlog_count(?:\{[^}]*\})?\s+(\d+)') {
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
