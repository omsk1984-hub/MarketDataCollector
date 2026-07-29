$proc = Get-Process -Name "MarketDataCollector.Worker" -ErrorAction SilentlyContinue
if ($proc) {
    taskkill /F /IM MarketDataCollector.Worker.exe
}

# Проверка Docker
$dockerRunning = docker info 2>&1 | Select-String "Server Version"
if (-not $dockerRunning) {
    Write-Host "Docker не запущен! Запустите Docker Desktop." -ForegroundColor Red
    Read-Host -Prompt "Нажмите любую клавишу для выхода"
    exit 1
}

# Проверка Kafka
$kafkaContainer = docker ps --filter "name=marketdata-kafka" --format "{{.Status}}" 2>&1
if (-not $kafkaContainer) {
    Write-Host "Kafka контейнер не запущен!" -ForegroundColor Yellow
    Write-Host "Запустите: docker-compose -f docker/docker-compose.yml up -d" -ForegroundColor Yellow
    Write-Host "Продолжаем без Kafka (свечи будут записываться напрямую в БД)..." -ForegroundColor Yellow
    Write-Host ""
}

Write-Host "Компиляция решения..." -ForegroundColor Cyan
dotnet build MarketDataCollector.sln
if ($LASTEXITCODE -ne 0) {
    Write-Host "Ошибка компиляции!" -ForegroundColor Red
    Read-Host -Prompt "Нажмите любую клавишу для выхода"
    exit 1
}

cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
dotnet run
Read-Host -Prompt "Нажмиете любую клавишу для выхода"