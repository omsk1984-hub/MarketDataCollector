param(
    [string]$Csv = "traces/counters_20260731_060048.csv",
    [string]$Out = "traces/analysis_summary.txt"
)

$ErrorActionPreference = "Stop"
$rows = Import-Csv -Path $Csv -Encoding UTF8

# Список уникальных метрик (без bucket/sum/count от histograms)
$metricTypes = $rows | ForEach-Object { $_.Metric } | Sort-Object -Unique
$sb = New-Object System.Text.StringBuilder
[void]$sb.AppendLine("UNIQUE_METRICS=" + $metricTypes.Count)
foreach ($m in $metricTypes) { [void]$sb.AppendLine("METRIC::" + $m) }

# Сэмплы (по timestamp)
$samples = $rows | ForEach-Object { $_.Timestamp } | Sort-Object -Unique
[void]$sb.AppendLine("")
[void]$sb.AppendLine("SAMPLES=" + $samples.Count)
[void]$sb.AppendLine("FIRST_SAMPLE=" + $samples[0])
[void]$sb.AppendLine("LAST_SAMPLE=" + $samples[$samples.Count-1])

# Группируем по timestamp для первой и последней точек
function Get-Sample {
    param($ts)
    return ($rows | Where-Object { $_.Timestamp -eq $ts })
}

# Ключевые runtime-метрики: первая и последняя точка
function Extract-Runtime {
    param($ts, $label)
    $s = Get-Sample $ts
    $alloc = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_allocations_size_bytes_total" } | Select-Object -First 1).Value
    $gcGen2 = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_collections_count_total" -and $_.Labels -match "gen2" } | Select-Object -First 1).Value
    $gcGen1 = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_collections_count_total" -and $_.Labels -match "gen1" } | Select-Object -First 1).Value
    $gcGen0 = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_collections_count_total" -and $_.Labels -match "gen0" } | Select-Object -First 1).Value
    $gcDur = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_duration_nanoseconds_total" } | Select-Object -First 1).Value
    $gcObj = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_objects_size_bytes" } | Select-Object -First 1).Value
    $heap = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_gc_heap_size_bytes" } | Select-Object -First 1).Value
    $exc = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_exceptions_count_total" } | Select-Object -First 1).Value
    $lock = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_monitor_lock_contention_count_total" } | Select-Object -First 1).Value
    $tpQueue = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_thread_pool_queue_length" } | Select-Object -First 1).Value
    $tpThreads = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_thread_pool_threads_count" } | Select-Object -First 1).Value
    $tpCompleted = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_thread_pool_completed_items_count_total" } | Select-Object -First 1).Value
    $timer = ($s | Where-Object { $_.Metric -eq "process_runtime_dotnet_timer_count" } | Select-Object -First 1).Value
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("=== RUNTIME @ $label ($ts) ===")
    [void]$sb.AppendLine("alloc_bytes_total=" + $alloc)
    [void]$sb.AppendLine("gc_collections gen0/gen1/gen2 = " + $gcGen0 + "/" + $gcGen1 + "/" + $gcGen2)
    [void]$sb.AppendLine("gc_duration_ns_total=" + $gcDur)
    [void]$sb.AppendLine("gc_objects_size_bytes=" + $gcObj)
    [void]$sb.AppendLine("gc_heap_size_bytes=" + $heap)
    [void]$sb.AppendLine("exceptions_total=" + $exc)
    [void]$sb.AppendLine("lock_contention_total=" + $lock)
    [void]$sb.AppendLine("tp_queue=" + $tpQueue + " tp_threads=" + $tpThreads + " tp_completed=" + $tpCompleted)
    [void]$sb.AppendLine("timer_count=" + $timer)
}

Extract-Runtime $samples[0] "FIRST"
Extract-Runtime $samples[$samples.Count-1] "LAST"

# Кастомные метрики MarketDataCollector - последний сэмпл, все с последней точки
$last = Get-Sample $samples[$samples.Count-1]
[void]$sb.AppendLine("")
[void]$sb.AppendLine("=== CUSTOM METRICS @ LAST SAMPLE ===")
$custom = $last | Where-Object { $_.Metric -notmatch "^process_runtime|^target_info" }
foreach ($m in ($custom | Sort-Object Metric, Labels)) {
    [void]$sb.AppendLine("M:" + $m.Metric + " | L:[" + $m.Labels + "] = " + $m.Value)
}

$sb.ToString() | Out-File -FilePath $Out -Encoding UTF8
Write-Output ("DONE -> " + $Out)
