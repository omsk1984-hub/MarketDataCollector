Remove-Item -Path _parse_check.ps1, _add_bom.ps1 -Force -ErrorAction SilentlyContinue
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
Write-Host "Temp files removed" -ForegroundColor Green