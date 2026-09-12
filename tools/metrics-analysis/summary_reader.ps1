# Разбор counters_*.csv (OpenTelemetry-Prometheus экспортер).
# Строка формата: "ts","metric","labels","type","value","desc"
# Надёжный regex для первых двух полей: ^"([^"]*)","([^"]*)"
# Результат пишется в plans/_metrics_out.txt.

$ErrorActionPreference = 'Stop'
$out = @()

function ExtractField($line) {
    # returns [ts, metric, labels, value] via regex; histogram buckets отдельно
    if (-not ($line -match '^"([^"]*)","([^"]*)","((?:[^"]|"")*)","([^"]*)","([^"]*)"')) { return $null }
    return @($Matches[1], $Matches[2], $Matches[3], $Matches[5])
}

foreach ($f in $args) {
    $out += "===== FILE: $f ====="
    if (-not (Test-Path $f)) { $out += "NOT FOUND"; continue }

    $lines = Get-Content $f -Encoding UTF8
    $out += ("TOTAL_LINES=" + $lines.Count)

    $samples = @{}
    $metrics = @{}
    $series = @{}   # key = metric + '###' + compactLabels  -> [ 'count', 'firstVal', 'lastVal', 'firstTs', 'lastTs' ]

    for ($i = 1; $i -lt $lines.Count; $i++) {
        $line = $lines[$i]
        $r = ExtractField $line
        if ($null -eq $r) { continue }
        $ts = $r[0]; $metric = $r[1]; $labels = $r[2]; $value = $r[3]

        $samples[$ts] = 1
        $metrics[$metric] = 1

        if ($metric -eq 'target_info') { continue }
        if ($metric -match '_bucket$') { continue }
        # Сокращаем labels до ключевых атрибутов
        $compact = ''
        if ($labels -match 'channel_index="([^"]*)"') { $compact += ';ch=' + $Matches[1] }
        if ($labels -match 'batch_size="([^"]*)"') { $compact += ';bs=' + $Matches[1] }
        if ($labels -match 'inserted_count="([^"]*)"') { $compact += ';ins=' + $Matches[1] }
        if ($labels -match 'generation="([^"]*)"') { $compact += ';gen=' + $Matches[1] }
        if ($labels -match 'exchange="([^"]*)"') { $compact += ';ex=' + $Matches[1] }
        if ($labels -match 'symbol="([^"]*)"') { $compact += ';sym=' + $Matches[1] }

        $key = $metric + '|' + $compact
        if (-not $series.ContainsKey($key)) {
            $series[$key] = @($ts, $value, $ts, $value, 1)
        } else {
            $s = $series[$key]
            $s[2] = $ts
            $s[3] = $value
            $s[4] = $s[4] + 1
            $series[$key] = $s
        }
    }

    $tss = $samples.Keys | Sort-Object
    $out += ("SAMPLES=" + $samples.Count)
    $out += ("FIRST_TS=" + $tss[0])
    $out += ("LAST_TS=" + $tss[$tss.Count - 1])
    $out += "=== UNIQUE METRIC NAMES ==="
    $metrics.Keys | Sort-Object | ForEach-Object { $out += $_ }
    $out += "=== SERIES (first -> last) ==="
    foreach ($k in ($series.Keys | Sort-Object)) {
        $s = $series[$k]
        $out += ($k + " | n=" + $s[4] + " | " + $s[0] + " -> " + $s[2] + " | val " + $s[1] + " -> " + $s[3])
    }
    $out += ""
}

Set-Content -Path "plans/_metrics_out.txt" -Value $out -Encoding UTF8
Write-Host "WROTE plans/_metrics_out.txt lines=$($out.Count)"