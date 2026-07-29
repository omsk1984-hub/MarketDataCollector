$proc = Get-Process -Name "MarketDataCollector.Worker" -ErrorAction SilentlyContinue
if ($proc) {
    taskkill /F /IM MarketDataCollector.Worker.exe
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