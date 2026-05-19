$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $projectDir "tests/MarketDataCollector.Tests")
dotnet test MarketDataCollector.Tests.csproj --nologo -v n "-property:NoWarn=CS*%3BNU*%3BMSB*"
Read-Host -Prompt "Нажмите любую клавишу для выхода"
