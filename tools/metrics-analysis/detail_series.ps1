# Детальный разбор key-метрик через надёжный regex (value = 5-я группа).
# Строка: "ts","metric","labels","type","value","desc"
# labels содержит ""," (двойные кавычки внутри) -> используем ((?:[^"]|"")*)
# Вывод полностью в plans/_metrics_detail.txt.

$ErrorActionPreference = 'Stop'
$out = @()

$pat = '^"([^"]*)","([^"]*)","((?:[^"]|"")*)","([^"]*)","([^"]*)"'

foreach ($f in $args) {
    $out += "===== DETAIL FILE: $f ====="
    $lines = Get-Content $f -Encoding UTF8
    $map = @{}
    # печатаем по каждому сэмплу только последние значения интересных метрик + первые для gc collections

    for ($i = 1; $i -lt $lines.Count; $i++) {
        $line = $lines[$i]
        if (-not ($line -match $pat)) { continue }
        $ts = $Matches[1]; $metric = $Matches[2]; $labels = $Matches[3]; $value = $Matches[5]

        $want = ($metric -match '^process_runtime_dotnet_gc_(collections_count_total|heap_size_bytes|committed_memory_size_bytes|heap_fragmentation_size_bytes)$') -or
            ($metric -eq 'ws_messages_received_count_total') -or
            ($metric -eq 'ticks_incoming_count_total') -or
            ($metric -eq 'ticks_received_count_total') -or
            ($metric -eq 'ticks_processed_count_total') -or
            ($metric -eq 'ticks_deduplicated_cache_count_total') -or
            ($metric -eq 'ticks_deduplicated_db_count_total') -or
            ($metric -eq 'ticks_dropped_count') -or
            ($metric -eq 'processor_channel_fill_level_count')
        if (-not $want) { continue }

        $key = "$metric|$labels"
        $map[$key] = @($ts, $value)
    }
    foreach ($k in ($map.Keys | Sort-Object)) {
        $v = $map[$k]
        $out += ($k + " | last=" + $v[1] + " @ " + $v[0])
    }
    $out += ""
}

Set-Content -Path "plans/_metrics_detail.txt" -Value $out -Encoding UTF8
Write-Host "WROTE plans/_metrics_detail.txt lines=$($out.Count)"