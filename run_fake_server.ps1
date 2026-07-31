# Параметры (можно менять под свои нужды)
$Port = 5000
$Rps = 20000
$Symbols = "btcusdt,ethusdt,solusdt"
$BasePrice = 5000
$MaxTicks = 1800000
$DupPercent = 3

# Останавливаем предыдущий экземпляр, если запущен
$proc = Get-Process -Name "FakeTickServer" -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "Останавливаю предыдущий FakeTickServer..." -ForegroundColor Yellow
    taskkill /F /IM FakeTickServer.exe 2>$null
    Start-Sleep -Seconds 1
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "    FakeTickServer - Load Generator" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Port:      $Port"
Write-Host "RPS:       $Rps"
Write-Host "Symbols:   $Symbols"
Write-Host "BasePrice: $BasePrice"
Write-Host "MaxTicks:  $MaxTicks (0=без лимита)"
Write-Host "DupPercent: $DupPercent% (0=все уникальны)"
Write-Host ""
Write-Host "Для подключения MarketDataCollector замените в appsettings.json:" -ForegroundColor Yellow
Write-Host "  wss://stream.binance.com:9443/ws/{symbol}@trade" -ForegroundColor Gray
Write-Host "  -> ws://localhost:$Port/ws/{symbol}@trade" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

cd tests/FakeTickServer

Write-Host "Компиляция FakeTickServer..." -ForegroundColor Yellow
dotnet build
if ($LASTEXITCODE -ne 0) {
    Write-Host "ОШИБКА КОМПИЛЯЦИИ! Запуск отменён." -ForegroundColor Red
    Read-Host -Prompt "Нажмите любую клавишу для выхода"
    exit 1
}
Write-Host "Компиляция успешна." -ForegroundColor Green
Write-Host ""

dotnet run -- --port $Port --rps $Rps --symbols $Symbols --base-price $BasePrice --max-ticks $MaxTicks --dup-percent $DupPercent

Read-Host -Prompt "Нажмите любую клавишу для выхода"
