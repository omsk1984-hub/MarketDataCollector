cd src/MarketDataCollector.Workers/MarketDataCollector.Worker
$proc = Get-Process -Name "MarketDataCollector.Worker" -ErrorAction SilentlyContinue
if ($proc) {
    taskkill /F /IM MarketDataCollector.Worker.exe
}
dotnet run
Read-Host -Prompt "Нажмиете любую клавишу для выхода"