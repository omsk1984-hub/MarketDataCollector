$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$files = @('docker/deploy-dev.ps1', 'docker/deploy-prod.ps1')
foreach ($f in $files) {
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $f).Path, [ref]$tokens, [ref]$parseErrors) | Out-Null
    Write-Host ("== {0} ==" -f $f)
    if ($parseErrors.Count -gt 0) {
        foreach ($e in $parseErrors) {
            Write-Host ("  ERR: {0}" -f $e.Message) -ForegroundColor Red
        }
    } else {
        Write-Host "  OK (syntax without errors)" -ForegroundColor Green
    }
}