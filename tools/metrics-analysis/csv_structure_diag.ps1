# Диагностика структуры строки CSV.
$ErrorActionPreference = 'Stop'
$lines = Get-Content 'traces/counters_20260912_205238.csv' -Encoding UTF8
$out = @()
for ($j = 0; $j -lt 6; $j++) {
    $line = $lines[$j + 1]
    $p = $line.Split('","')
    $out += ("LINE idx=$j parts.Count=$($p.Count)")
    for ($k = 0; $k -lt [Math]::Min(6, $p.Count); $k++) {
        $v = $p[$k]
        $out += ("  parts[$k]='" + $v + "'  LEN=$($v.Length)")
    }
}
Set-Content -Path "plans/_diag.txt" -Value $out -Encoding UTF8
Write-Host "WROTE diagnostics"