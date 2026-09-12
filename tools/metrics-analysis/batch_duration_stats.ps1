# Статистика записи батчей: inserted_count распределение + длительности.
$ErrorActionPreference = 'Stop'
$lines = Get-Content 'traces/counters_20260912_205238.csv' -Encoding UTF8
$out = @()
$pat = '^"([^"]*)","([^"]*)","((?:[^"]|"")*)","([^"]*)","([^"]*)"'
$insVals = @(); $durVals = @()
$insDist = @{}
for ($i = 1; $i -lt $lines.Count; $i++) {
    if (-not ($lines[$i] -match $pat)) { continue }
    $metric = $Matches[2]; $labels = $Matches[3]; $value = $Matches[5]
    if ($metric -eq 'ticks_batch_write_duration_milliseconds_sum') {
        $ins = ''
        if ($labels -match 'inserted_count=""([0-9]+)""') { $ins = $Matches[1] }
        $insVals += [int]$ins
        $durVals += [double]$value
        if ($insDist.ContainsKey($ins)) { $insDist[$ins] = $insDist[$ins] + 1 } else { $insDist[$ins] = 1 }
    }
}
$out += ("TOTAL batches (count of sum series)=" + $insVals.Count)
$insVals = $insVals | Sort-Object
$sumIns = 0; foreach ($v in $insVals) { $sumIns += $v }
$out += ("inserted_count: min=" + $insVals[0] + " max=" + $insVals[$insVals.Count-1] + " avg=" + [Math]::Round($sumIns / $insVals.Count))
$durVals = $durVals | Sort-Object
$sumDur = 0; foreach ($v in $durVals) { $sumDur += $v }
$out += ("duration ms: min=" + $durVals[0] + " max=" + $durVals[$durVals.Count-1] + " avg=" + [Math]::Round($sumDur / $durVals.Count, 1) + " p50-like=" + $durVals[[int]($durVals.Count/2)])
# топовая десятка по inserted
$top = @()
foreach ($k in ($insDist.Keys | Sort-Object -Descending -Property { [int]$_ })) { $top += ("inserted=" + $k + " batches=" + $insDist[$k]) }
$out += "--- inserted distribution (top) ---"
foreach ($t in $top) { $out += $t }
Set-Content -Path "plans/_batch_dur.txt" -Value $out -Encoding UTF8
Write-Host "WROTE plans/_batch_dur.txt lines=$($out.Count)"