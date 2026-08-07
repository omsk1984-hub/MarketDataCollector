<#
.SYNOPSIS
    Развёртывание и управление dev-стеком MarketDataCollector через Docker Compose.

.DESCRIPTION
    Обёртка над `docker compose` для docker/docker-compose.yml (single-node Kafka,
    PostgreSQL, Prometheus, Alertmanager, blackbox-exporter, Aspire Dashboard, Seq, worker).

    Скрипт сам переходит в каталог docker/, поэтому относительные пути
    (./prometheus/..., build context .. и docker/.env) резолвятся корректно.

.PARAMETER Action
    Действие:
      up    — собрать (если нужно) и запустить все сервисы в фоне (по умолчанию)
      down  — остановить и удалить контейнеры (без удаления volume)
      downV — остановить и удалить контейнеры вместе с volume (полный сброс данных)
      restart — перезапустить все сервисы
      logs  — вывести и «подписаться» на логи всех сервисов
      ps    — показать статус контейнеров
      pull  — обновить образы
      build — пересобрать образ worker

.PARAMETER Service
    Имя конкретного сервиса (например: worker, kafka, postgres). Применяется к
    up/down/restart/logs. Если не задан — операция выполняется над всем стеком.

.PARAMETER NoBuild
    При up не пересобирать образ worker (использовать существующий).

.PARAMETER ForceRecreate
    При up пересоздать контейнеры даже при отсутствии изменений конфигурации.

.EXAMPLE
    .\deploy-dev.ps1
    .\deploy-dev.ps1 -Action up
    .\deploy-dev.ps1 -Action up -Service worker -ForceRecreate
    .\deploy-dev.ps1 -Action down
    .\deploy-dev.ps1 -Action logs
#>

[CmdletBinding()]
param(
    [ValidateSet("up", "down", "downV", "restart", "logs", "ps", "pull", "build")]
    [string]$Action = "up",
    [string]$Service = "",
    [switch]$NoBuild,
    [switch]$ForceRecreate
)

$ErrorActionPreference = "Stop"

# Гарантируем корректный вывод кириллицы в консоль (UTF-8).
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Каталог скрипта (docker/) — рабочая точка для docker compose.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $scriptDir "docker-compose.yml"

# Проверка запущенного Docker (демон отвечает на docker info).
$dockerRunning = docker info 2>&1 | Select-String "Server Version"
if (-not $dockerRunning) {
    Write-Host "Docker не запущен! Запустите Docker Desktop и повторите." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $composeFile)) {
    Write-Host "Не найден файл compose: $composeFile" -ForegroundColor Red
    exit 1
}

Push-Location $scriptDir
try {
    switch ($Action) {
        "up" {
            $dockerArgs = @("compose", "-f", "docker-compose.yml", "up", "-d")
            if (-not $NoBuild) {
                $dockerArgs += "--build"
            }
            if ($ForceRecreate) {
                $dockerArgs += "--force-recreate"
            }
            if ($Service) {
                $dockerArgs += $Service
            }
            Write-Host "=== Запуск dev-стека (up -d) ===" -ForegroundColor Cyan
            docker @dockerArgs
            if ($LASTEXITCODE -ne 0) {
                Write-Host "docker compose up завершился с ошибкой." -ForegroundColor Red
                exit 1
            }
            Write-Host "" -ForegroundColor Cyan
            Write-Host "=== Статус контейнеров ===" -ForegroundColor Cyan
            docker compose -f docker-compose.yml ps
        }
        "down" {
            Write-Host "=== Остановка dev-стека (контейнеры, volume сохраняются) ===" -ForegroundColor Cyan
            docker compose -f docker-compose.yml down
        }
        "downV" {
            Write-Host "=== Полный сброс dev-стека (контейнеры + volume) ===" -ForegroundColor Yellow
            Write-Host "ВНИМАНИЕ: будут удалены все данные (PostgreSQL, Kafka, Prometheus, Seq)." -ForegroundColor Yellow
            docker compose -f docker-compose.yml down -v
        }
        "restart" {
            Write-Host "=== Перезапуск dev-стека ===" -ForegroundColor Cyan
            if ($Service) {
                docker compose -f docker-compose.yml restart $Service
            } else {
                docker compose -f docker-compose.yml restart
            }
        }
        "logs" {
            Write-Host "=== Логи dev-стека (Ctrl+C для выхода) ===" -ForegroundColor Cyan
            if ($Service) {
                docker compose -f docker-compose.yml logs -f --tail=100 $Service
            } else {
                docker compose -f docker-compose.yml logs -f --tail=100
            }
        }
        "ps" {
            Write-Host "=== Статус контейнеров dev-стека ===" -ForegroundColor Cyan
            docker compose -f docker-compose.yml ps
        }
        "pull" {
            Write-Host "=== Обновление образов dev-стека ===" -ForegroundColor Cyan
            docker compose -f docker-compose.yml pull
        }
        "build" {
            Write-Host "=== Сборка образа worker ===" -ForegroundColor Cyan
            docker compose -f docker-compose.yml build worker
        }
    }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Готово." -ForegroundColor Green
