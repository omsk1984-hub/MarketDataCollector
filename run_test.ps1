# Собирает покрытие кода и выводит краткую сводку (% строк и % веток) в консоль.
# Требований к внешним инструментам нет: coverlet.collector уже подключён в тестовом проекте.

$ErrorActionPreference = "Stop"

# Гарантируем корректный вывод кириллицы в консоль (UTF-8).
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$testDir = Join-Path $projectDir "tests/MarketDataCollector.Tests"
$resultsDir = Join-Path $projectDir "TestResults"

# Порог минимального покрытия строк (в процентах). 0 = без падения процесса.
$CoverageThreshold = 0

Set-Location $testDir

Write-Host "=== Запуск тестов со сбором покрытия ===" -ForegroundColor Cyan

# Собираем покрытие через встроенный коллектор. Старые артефакты очищаем,
# чтобы выбрать свежий coverage.cobertura.xml.
if (Test-Path $resultsDir) {
    Remove-Item -Path $resultsDir -Recurse -Force
}

# %3B здесь — MSBuild-декодирование ';' внутри значения NoWarn (иначе свойство разобьётся).
dotnet test MarketDataCollector.Tests.csproj --nologo -v q `
    --property:NoWarn=CS*%3BNU*%3BMSB* `
    --collect:"XPlat Code Coverage" `
    --results-directory $resultsDir

# Выбираем самый свежий отчёт Cobertura.
$coverageFile = Get-ChildItem -Path $resultsDir -Recurse -Filter "coverage.cobertura.xml" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if ($null -eq $coverageFile) {
    Write-Host "Не найден coverage.cobertura.xml. Проверьте, что coverlet.collector подключён в тестовом проекте." -ForegroundColor Yellow
    exit 1
}

# Парсим отчёт: в корневом элементе <coverage> coverlet отдаёт line-rate и branch-rate
# как долю в диапазоне 0..1.
[xml]$coverage = Get-Content -Path $coverageFile.FullName -Raw

$lineRate = [double]$coverage.coverage.'line-rate'
$branchRate = [double]$coverage.coverage.'branch-rate'

$linePercent = $lineRate * 100
$branchPercent = $branchRate * 100

Write-Host ""
Write-Host "=== Сводка по покрытию ===" -ForegroundColor Cyan
Write-Host ("  Строки: {0:N1}%   Ветки: {1:N1}%" -f $linePercent, $branchPercent)

# Топ-10 файлов с наименьшим процентом покрытия строк.
# В Cobertura-отчёте coverlet каждый <class> имеет атрибут filename и блок
# <lines valid="N" covered="M"/>. Группируем по файлу и суммируем строки.
Write-Host ""
Write-Host "=== Топ-10 файлов с наименьшим покрытием строк ===" -ForegroundColor Cyan

$filesByClass = @{}
foreach ($package in $coverage.coverage.packages.package) {
    foreach ($classNode in $package.classes.class) {
        $filename = $classNode.filename
        if ([string]::IsNullOrWhiteSpace($filename)) {
            continue
        }
        # Нормализуем разделители пути к единому виду (заменяем / на \).
        $normalized = $filename.Replace('/', '\')
        $linesNode = $classNode.lines
        $valid = 0
        $covered = 0
        if ($null -ne $linesNode -and $null -ne $linesNode.valid) {
            $valid = [int]$linesNode.valid
        }
        if ($null -ne $linesNode -and $null -ne $linesNode.covered) {
            $covered = [int]$linesNode.covered
        }
        if (-not $filesByClass.ContainsKey($normalized)) {
            $filesByClass[$normalized] = @{ Valid = 0; Covered = 0 }
        }
        $filesByClass[$normalized].Valid += $valid
        $filesByClass[$normalized].Covered += $covered
    }
}

$fileStats = foreach ($entry in $filesByClass.GetEnumerator()) {
    if ($entry.Value.Valid -eq 0) {
        continue  # файл без строк кода — не участвует в топе
    }
    $percent = ($entry.Value.Covered / $entry.Value.Valid) * 100
    [PSCustomObject]@{
        Percent = $percent
        Covered = $entry.Value.Covered
        Valid   = $entry.Value.Valid
        File    = $entry.Key
    }
}

$topUncovered = $fileStats |
    Sort-Object Percent, Valid |
    Select-Object -First 10

if ($topUncovered.Count -eq 0) {
    Write-Host "  Нет файлов с кодом для оценки." -ForegroundColor Yellow
} else {
    foreach ($f in $topUncovered) {
        if ($f.Percent -eq 0) {
            $marker = "БЕЗ ПОКРЫТИЯ"
        } else {
            $marker = ""
        }
        Write-Host ("  {0,6:N1}%  {1,5}/{2,-5}  {3} {4}" -f $f.Percent, $f.Covered, $f.Valid, $f.File, $marker)
    }
}

if ($linePercent -lt $CoverageThreshold) {
    Write-Host ("Вердикт: FAIL (ниже порога {0}%)" -f $CoverageThreshold) -ForegroundColor Red
    exit 1
} else {
    Write-Host "Вердикт: OK" -ForegroundColor Green
}
