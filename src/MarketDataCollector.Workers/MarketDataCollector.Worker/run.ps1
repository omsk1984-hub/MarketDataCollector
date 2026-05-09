cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
taskkill /F /IM MarketDataCollector.Worker.exe 
dotnet run
Read-Host -Prompt "Нажмиете любую клавишу для выхода"