<#
.SYNOPSIS
    Компиляция проекта tools/Profiler (MarketDataCollector.Profiler).

.DESCRIPTION
    Собирает standalone .NET 8 утилиту профилирования. Проект не входит в
    MarketDataCollector.sln и собирается отдельно, поэтому скрипт запускает
    dotnet build напрямую по пути Profiler.csproj.

.PARAMETER Configuration
    Конфигурация сборки: Debug (по умолчанию) или Release.

.PARAMETER OutputDir
    Директория вывода. По умолчанию bin/$Configuration/net8.0 относительно проекта.

.PARAMETER NoRestore
    Пропустить восстановление NuGet-пакетов.

.EXAMPLE
    .\compile.ps1
    .\compile.ps1 -Configuration Release -OutputDir ..\..\artifacts\profiler
#>

[CmdletBinding()]
param(
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Debug",

    [string]$OutputDir = "",

    [switch]$NoRestore
)

$ErrorActionPreference = "Stop"

# Корень скрипта (tools/Profiler) — работаем относительно него
$scriptDir = $PSScriptRoot
$projectPath = Join-Path $scriptDir "Profiler.csproj"

if (-not (Test-Path $projectPath)) {
    Write-Host "Не найден файл проекта: $projectPath" -ForegroundColor Red
    exit 1
}

Write-Host "Компиляция Profiler ($Configuration)..." -ForegroundColor Cyan

$arguments = @(
    "build",
    "`"$projectPath`"",
    "--configuration", $Configuration
)

if ($NoRestore) {
    $arguments += "--no-restore"
}

if ($OutputDir) {
    $arguments += "--output", "`"$OutputDir`""
}

# Обёртка cmd /c даёт стабильную кодировку кириллицы (правило проекта)
chcp 65001 >nul & cmd /c "dotnet $($arguments -join ' ')"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Ошибка компиляции!" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "Сборка успешно завершена." -ForegroundColor Green
exit 0
