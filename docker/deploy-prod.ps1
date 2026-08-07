<#
.SYNOPSIS
    Развёртывание и управление production-стеком MarketDataCollector через Docker Compose.

.DESCRIPTION
    Обёртка над `docker compose` для docker/docker-compose.prod.yml
    (3-нодный Kafka-кластер, PostgreSQL со SCRAM-аутентификацией, Prometheus,
    Alertmanager, blackbox-exporter, Aspire Dashboard, Seq, worker из GHCR).

    ВАЖНО: производственные секреты задаются ТОЛЬКО через файл docker/.env
    (см. docker/.env.example). Скрипт проверяет его наличие и не даёт продолжить,
    если он отсутствует.

    Скрипт сам переходит в каталог docker/, поэтому относительные пути
    (./prometheus/... и docker/.env) резолвятся корректно.

.PARAMETER Action
    Действие:
      up    — подтянуть образы и запустить все сервисы в фоне (по умолчанию)
      down  — остановить и удалить контейнеры (без удаления volume)
      downV — остановить и удалить контейнеры вместе с volume (полный сброс данных)
      restart — перезапустить все сервисы
      logs  — вывести и «подписаться» на логи всех сервисов
      ps    — показать статус контейнеров
      pull  — обновить образы (включая worker из GHCR)
      config — показать итоговую конфигурацию после подстановки .env (без запуска)

.PARAMETER Service
    Имя конкретного сервиса (например: worker, kafka-1, postgres). Применяется к
    up/down/restart/logs. Если не задан — операция выполняется над всем стеком.

.PARAMETER NoPull
    При up не выполнять `docker compose pull` перед запуском (использовать локальные образы).

.PARAMETER ForceRecreate
    При up пересоздать контейнеры даже при отсутствии изменений конфигурации.

.EXAMPLE
    .\deploy-prod.ps1
    .\deploy-prod.ps1 -Action up -Service worker
    .\deploy-prod.ps1 -Action down
    .\deploy-prod.ps1 -Action logs
    .\deploy-prod.ps1 -Action config
#>

[CmdletBinding()]
param(
    [ValidateSet("up", "down", "downV", "restart", "logs", "ps", "pull", "config")]
    [string]$Action = "up",
    [string]$Service = "",
    [switch]$NoPull,
    [switch]$ForceRecreate
)

$ErrorActionPreference = "Stop"

# Гарантируем корректный вывод кириллицы в консоль (UTF-8).
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Каталог скрипта (docker/) — рабочая точка для docker compose.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $scriptDir "docker-compose.prod.yml"
$envFile = Join-Path $scriptDir ".env"

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

# В production секреты обязательны. docker compose и так упадёт на :?-переменных,
# но проверяем раньше и даём понятную подсказку.
if (-not (Test-Path $envFile)) {
    Write-Host "Не найден файл секретов: $envFile" -ForegroundColor Red
    Write-Host "Скопируйте docker/.env.example в docker/.env и заполните реальными значениями." -ForegroundColor Yellow
    exit 1
}

Push-Location $scriptDir
try {
    switch ($Action) {
        "up" {
            $dockerArgs = @("compose", "-f", "docker-compose.prod.yml", "up", "-d")
            if (-not $NoPull) {
                Write-Host "=== Подтягивание образов (pull) ===" -ForegroundColor Cyan
                docker compose -f docker-compose.prod.yml pull
                if ($LASTEXITCODE -ne 0) {
                    Write-Host "docker compose pull завершился с ошибкой." -ForegroundColor Red
                    exit 1
                }
            }
            if ($ForceRecreate) {
                $dockerArgs += "--force-recreate"
            }
            if ($Service) {
                $dockerArgs += $Service
            }
            Write-Host "=== Запуск production-стека (up -d) ===" -ForegroundColor Cyan
            docker $dockerArgs
            if ($LASTEXITCODE -ne 0) {
                Write-Host "docker compose up завершился с ошибкой." -ForegroundColor Red
                exit 1
            }
            Write-Host "" -ForegroundColor Cyan
            Write-Host "=== Статус контейнеров ===" -ForegroundColor Cyan
            docker compose -f docker-compose.prod.yml ps
        }
        "down" {
            Write-Host "=== Остановка production-стека (контейнеры, volume сохраняются) ===" -ForegroundColor Cyan
            docker compose -f docker-compose.prod.yml down
        }
        "downV" {
            Write-Host "=== Полный сброс production-стека (контейнеры + volume) ===" -ForegroundColor Yellow
            Write-Host "ВНИМАНИЕ: будут удалены ВСЕ данные (PostgreSQL, Kafka x3, Prometheus, Seq)." -ForegroundColor Yellow
            docker compose -f docker-compose.prod.yml down -v
        }
        "restart" {
            Write-Host "=== Перезапуск production-стека ===" -ForegroundColor Cyan
            if ($Service) {
                docker compose -f docker-compose.prod.yml restart $Service
            } else {
                docker compose -f docker-compose.prod.yml restart
            }
        }
        "logs" {
            Write-Host "=== Логи production-стека (Ctrl+C для выхода) ===" -ForegroundColor Cyan
            if ($Service) {
                docker compose -f docker-compose.prod.yml logs -f --tail=100 $Service
            } else {
                docker compose -f docker-compose.prod.yml logs -f --tail=100
            }
        }
        "ps" {
            Write-Host "=== Статус контейнеров production-стека ===" -ForegroundColor Cyan
            docker compose -f docker-compose.prod.yml ps
        }
        "pull" {
            Write-Host "=== Обновление образов production-стека ===" -ForegroundColor Cyan
            docker compose -f docker-compose.prod.yml pull
        }
        "config" {
            Write-Host "=== Итоговая конфигурация production-стека (после подстановки .env) ===" -ForegroundColor Cyan
            docker compose -f docker-compose.prod.yml config
        }
    }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Готово." -ForegroundColor Green
