# План: исправление «potentially broken trace» при конвертации SpeedScope

## Проблема

При запуске `run_all_metrics.ps1` сбор trace завершился ошибкой:

```
Detected a potentially broken trace. Continuing with best-efforts to convert the trace, but resulting in broken stacks as a result.
Conversion complete
[!] Ошибка конвертации trace
```

Файл `allocation_trace_*.nettrace` оказался битым (не финализированным), поэтому `dotnet-trace convert --format speedscope` не смог собрать корректные стеки аллокаций. Сами counters CSV и оба gcdump собрались нормально.

## Корневая причина

1. [`Start-TraceCollection`](scripts/common-functions.ps1:161) запускает `dotnet-trace collect` **без `--duration`** (аргументы на строке 190).
2. [`Stop-TraceCollection`](scripts/common-functions.ps1:206) останавливает процесс через `taskkill /PID` (строка 225) и затем `$TraceProcess.Kill()` (строка 238) — это жёсткое убийство, а не корректный `Ctrl+C`.
3. dotnet-trace **финализирует файл `.nettrace` только при получении `Ctrl+C`**. При принудительном убийстве поток событий обрывается, файл остаётся незакрытым → `convert` видит битый trace.
4. [`Convert-TraceToSpeedScope`](scripts/common-functions.ps1:293) скрывает детали: при сбое выводит только общее `[!] Ошибка конвертации trace`, не показывая предупреждение о битом trace.

## Исправления в scripts/common-functions.ps1

### 1. `Start-TraceCollection` — добавить `--duration`

Строка 190:
```powershell
$psi.Arguments = "collect --process-id $ProcessId --profile gc-verbose --output `"$OutputFilePath`""
```
Заменить на:
```powershell
$psi.Arguments = "collect --process-id $ProcessId --profile gc-verbose --duration $DurationSec --output `"$OutputFilePath`""
```

Эффект: dotnet-trace корректно завершится сам через `$DurationSec` секунд и финализирует файл. Принудительная остановка становится лишь страховкой.

### 2. `Stop-TraceCollection` — грациозная остановка

Заменить тело функции так, чтобы первичным было отправление `Ctrl+C` (через `GenerateConsoleCtrlEvent` / посылку сигнала), а `Kill()` — только фолбэк.

Для корректного `Ctrl+C` в консольную группу процесса на Windows нужно:
- `AttachConsole(pid)` → `GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)` → `FreeConsole()`.
- Это требует P/Invoke через `Add-Type`.

Схема новой функции:

```powershell
function Stop-TraceCollection {
    param(
        [System.Diagnostics.Process]$TraceProcess,
        [int]$WaitSeconds = 15
    )

    if ($TraceProcess -eq $null -or $TraceProcess.HasExited) {
        Write-Host "[*] dotnet-trace уже завершён." -ForegroundColor DarkGray
        return
    }

    Write-Host "[*] Остановка dotnet-trace (PID: $($TraceProcess.Id))..." -ForegroundColor Yellow

    # 1. Отправляем Ctrl+C в консольную группу процесса dotnet-trace
    try {
        Add-Type -Namespace Win32 -Name ConsoleCtrl -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)]
public static extern bool AttachConsole(uint dwProcessId);
[DllImport("kernel32.dll", SetLastError = true)]
public static extern bool FreeConsole();
[DllImport("kernel32.dll", SetLastError = true)]
public static extern bool GenerateConsoleCtrlEvent(uint dwCtrlEvent, uint dwProcessGroupId);
public const uint CTRL_C_EVENT = 0;
'@
        # dotnet-trace — managed-процесс с консолью; пробуем AttachConsole
        $attached = [Win32.ConsoleCtrl]::AttachConsole([uint32]$TraceProcess.Id)
        if ($attached) {
            $null = [Win32.ConsoleCtrl]::GenerateConsoleCtrlEvent([Win32.ConsoleCtrl]::CTRL_C_EVENT, 0)
            Start-Sleep -Milliseconds 500
            $null = [Win32.ConsoleCtrl]::FreeConsole()
        }
    } catch {
        Write-Host "  Ctrl+C не удалось отправить: $($_.Exception.Message)" -ForegroundColor DarkGray
    }

    # 2. Ждём завершения
    if ($TraceProcess.WaitForExit($WaitSeconds * 1000)) {
        Write-Host "[+] dotnet-trace остановлен (Ctrl+C)." -ForegroundColor Green
        return
    }

    # 3. Фолбэк: taskkill без /F (более мягкий) затем Kill()
    Write-Host "[!] dotnet-trace не завершился, пробую taskkill..." -ForegroundColor Yellow
    try {
        & taskkill /PID $TraceProcess.Id 2>$null | Out-Null
    } catch { }
    if ($TraceProcess.WaitForExit(5000)) {
        Write-Host "[+] dotnet-trace остановлен." -ForegroundColor Green
        return
    }

    # 4. Последний фолбэк
    Write-Host "[!] Принудительное завершение dotnet-trace..." -ForegroundColor Yellow
    try {
        $TraceProcess.Kill()
        $TraceProcess.WaitForExit(5000)
        Write-Host "[-] dotnet-trace принудительно завершён." -ForegroundColor DarkGray
    } catch {
        Write-Host "[!] Не удалось завершить dotnet-trace: $($_.Exception.Message)" -ForegroundColor Red
    }
}
```

> **Примечание по приоритету решения.** Если `--duration` добавлен (п.1), то в штатном сценарии `dotnet-trace` завершится сам, и `Stop-TraceCollection` сработает на ветке «уже завершён» либо вообще не вызовется. `Ctrl+C` — дополнительная гарантия целостности файла, если duration не был задан (например, при ручном запуске `collect-trace.ps1`).

### 3. `Convert-TraceToSpeedScope` — детальный вывод

Строки 311-319 заменить на захват вывода конвертации и отображение предупреждения:

```powershell
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
```

## Исправления в scripts/collect-all.ps1

### 4. Порядок ожидания trace

Сейчас секция [4](scripts/collect-all.ps1:188-204) ждёт `$remaining` секунд и останавливает trace через `Stop-TraceCollection` (строка 209). После добавления `--duration` trace должен завершиться сам. Чтобы не дёргать принудительную остановку и гарантированно дождаться финализации:

- В цикле ожидания уже есть проверка `if ($traceJob.Process.HasExited) { break }` (строка 196) — оставить.
- В `Stop-TraceCollection` сначала вызвать `$TraceProcess.WaitForExit($WaitSeconds*1000)` и только при необходимости слать Ctrl+C. (Это уже учтено в п.2.)
- Дополнительно после остановки добавить короткую паузу `Start-Sleep -Seconds 2` перед конвертацией, чтобы dotnet-trace успел дописать финализирующие записи в `.nettrace`.

## Критерий приёмки

1. Повторный запуск `run_all_metrics.ps1` не выводит `Detected a potentially broken trace`.
2. `allocation_trace_*.nettrace` конвертируется в `.speedscope.json`, который открывается в speedscope.app с корректными стеками по профилю `GCAllocationTick`.
3. В выводе появляется `[+] SpeedScope файл создан`, а не `[!] Ошибка конвертации trace`.
4. Counters CSV и оба gcdump собираются как раньше.
