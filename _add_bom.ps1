$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$files = @('docker/deploy-dev.ps1', 'docker/deploy-prod.ps1', '_parse_check.ps1')
foreach ($f in $files) {
    $full = (Resolve-Path $f).Path
    $content = [System.IO.File]::ReadAllText($full, [System.Text.Encoding]::UTF8)
    # Перезаписываем с UTF-8 BOM (предисловие), чтобы парсилось в Windows PowerShell 5.1.
    $utf8Bom = New-Object System.Text.UTF8Encoding($true)
    [System.IO.File]::WriteAllText($full, $content, $utf8Bom)
    Write-Host ("BOM added: {0}" -f $f) -ForegroundColor Green
}