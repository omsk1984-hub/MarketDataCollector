# Optimization Plan v2: MarketDataCollector (Исправленный)

**Based on:** counters-analysis-20260730-133957.md (run 2: 1.2M ticks, ~18K ticks/sec)
**Goal:** increase write throughput, reduce latency, eliminate losses
**Current metrics:** ~17,634 ticks/sec, batch write 150-200ms (outliers up to 1.57s), 6K DropOldest, 29,869 timers, 41 exceptions, 4 Gen2 GC in 66 sec

**Changes from v1:** Исправлен диагноз LOH, добавлены пропущенные рекомендации, расширена классификация исключений

---

## Current Problems Analysis

### Problem 1: Retry/backoff only for deadlock (1 attempt)

**Source:** RawTickRepository.BulkCopyAsync() — retry loop with 1 attempt

**Current behavior** (строки 359-376):
- Only 1 retry for deadlock (40P01) and NpgsqlException
- Fixed delay 200ms without exponential backoff
- Outliers up to 1.57s not handled

**Solution:** Add retry with exponential backoff (3-5 attempts, 100ms to 1600ms) for transient errors.

### Problem 2: Abnormally many timers (29,869)

**Timer sources in production:**

| Source | File | Count | Type |
|--------|------|-------|------|
| Task.Delay flush timer | MarketDataProcessor.cs:411 | ~600+ созданий | Internal timer (new per iteration) |
| MonitoringService._statusTimer | MonitoringService.cs:38 | 1 | System.Threading.Timer |
| TickAggregator._flushTimer | TickAggregator.cs:156 | 0-1 | System.Threading.Timer |
| OTel RuntimeInstrumentation | Program.cs:30 | ~29,000 | ETW/EventSource |

**Root cause:** `Task.Delay(TimeSpan.FromSeconds(_flushIntervalSeconds), cancellationToken)` на строке 411 создаёт **новый internal timer** каждую итерацию цикла, где `batch.Count > 0`. При ~600 batch-ах это几百 timer creations. Основной источник (~29,000) — `AddRuntimeInstrumentation()`.

### Problem 3: 41 exceptions in 66 sec

**Current behavior** (MarketDataProcessor.cs:636-643):
- Single generic `catch (Exception ex)` — no classification
- Logs only "Критическая ошибка при обработке батча"

**Current behavior** (RawTickRepository.cs:368-375):
- Retry loop catches `PostgresException` (40P01) and `NpgsqlException`
- **No logging** in retry path — attempts are silently consumed

**Possible sources:** Npgsql timeout, ChannelClosedException, Connection broken, TaskCanceledException

### Problem 4: Gen2 GC 4 times in 66 sec (281 ms pause)

**Actual source analysis (CORRECTED):**
- Arrays in BulkCopyAsync (Guid[2000], string[2000], decimal[2000]) are **NOT LOH objects**
  - `Guid[2000]` = 2000 × 16 bytes = **32 KB** < 85 KB LOH threshold
  - `string[2000]` = 2000 × 8 bytes = **16 KB** < 85 KB LOH threshold
  - `decimal[2000]` = 2000 × 16 bytes = **32 KB** < 85 KB LOH threshold
- **Real LOH sources (16.9–21.9 MB):** Npgsql internal buffers, EF Core change tracker, HTTP/WebSocket buffers
- **Allocation rate:** ~44.5 MB/sec — drives Gen2 collections

---

## Optimization Plan

### Step 0: Increase batch size (priority: high, complexity: low)

**Goal:** Reduce DB write frequency from ~600 batches to ~240 batches per run.

**File:** [MarketDataProcessorOptions.cs](src/MarketDataCollector.Core/Configuration/MarketDataProcessorOptions.cs) + [appsettings.json](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json)

**Change:** Increase `BatchSize` from 2,000 to 5,000:

```json
{
  "MarketDataProcessor": {
    "BatchSize": 5000
  }
}
```

**Expected effect:**
- Batch count: ~600 → ~240 (-60%)
- Fewer DB round-trips = lower lock contention
- Risk: larger batches may increase individual write latency

**Validation:** Check that average batch write stays < 500ms with larger batches.

---

### Step 1: Retry/backoff for batch write (priority: high)

**Goal:** Eliminate outliers up to 1.57s, increase write reliability.

**File:** [RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:359)

**Change:** 3 attempts with exponential backoff + jitter:

```csharp
private const int BulkCopyMaxRetries = 3;
private static readonly TimeSpan BulkCopyBaseDelay = TimeSpan.FromMilliseconds(100);

int attempt = 0;
while (true)
{
    cancellationToken.ThrowIfCancellationRequested();
    try
    {
        return await _context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);
    }
    catch (Exception ex) when (IsTransient(ex) && attempt < BulkCopyMaxRetries)
    {
        attempt++;
        var delay = BulkCopyBaseDelay * (int)Math.Pow(2, attempt - 1);
        var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next(100));
        _logger.LogWarning(ex,
            "BulkCopy attempt {Attempt}/{MaxRetries} failed with {ExceptionType}, " +
            "SqlState={SqlState}, retrying after {Delay}ms, batch size={Count}",
            attempt, BulkCopyMaxRetries, ex.GetType().Name,
            (ex is PostgresException pg ? pg.SqlState : null),
            (delay + jitter).TotalMilliseconds, ticks.Count);
        await Task.Delay(delay + jitter, cancellationToken);
    }
}

private static bool IsTransient(Exception ex) =>
    (ex is PostgresException pg && pg.SqlState is "40P01" or "57014" or "08006" or "08001" or "08003")
    || ex is NpgsqlException
    || ex is TimeoutException;
```

**Expected effect:** Outliers > 1s reduced by ~80%.

---

### Step 2: Timer optimization (priority: medium)

**Goal:** Reduce timer count from 29,869 to < 100.

#### 2.1 Replace Task.Delay flush timer with System.Threading.Timer

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:408)

**Change:** Use System.Threading.Timer with reuse instead of Task.Delay per iteration:

```csharp
using var flushTimerCts = new CancellationTokenSource();
Timer? flushTimer = null;
if (_flushIntervalSeconds > 0)
{
    flushTimer = new Timer(_ => flushTimerCts.Cancel(),
        null, Timeout.Infinite, Timeout.Infinite);
}

try
{
    while (!cancellationToken.IsCancellationRequested)
    {
        if (_flushIntervalSeconds > 0 && batch.Count > 0)
        {
            var readTaskTyped = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
            flushTimer!.Change(TimeSpan.FromSeconds(_flushIntervalSeconds), Timeout.InfiniteTimeSpan);
            var flushDelay = Task.Delay(Timeout.Infinite, flushTimerCts.Token);
            var completed = await Task.WhenAny(readTaskTyped, flushDelay).ConfigureAwait(false);
        }
    }
}
finally
{
    flushTimer?.Dispose();
}
```

**Expected effect:** Task.Delay timer creations: ~600+ → ~3 (reuse).

#### 2.2 Consider disabling AddRuntimeInstrumentation

**File:** [Program.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs:30)

If OTel GC/ThreadPool metrics are not critical, disabling reduces timers from ~29,000 to < 100.

```csharp
// Заменить:
.AddRuntimeInstrumentation()
// На: (ничего — просто удалить строку)
```

**Trade-off:** Loses GC/ThreadPool metrics in Prometheus/Dashboard.

---

### Step 3: Exception investigation and classification (priority: medium)

**Goal:** Classify 41 exceptions, identify source, enable real-time monitoring.

#### 3.1 Typed exception logging in MarketDataProcessor

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:636)

**Change:** Replace generic catch with typed exception handling:

```csharp
catch (PostgresException pgEx)
{
    activity?.SetStatus(ActivityStatusCode.Error, pgEx.Message);
    _logger.LogError(pgEx,
        "PostgreSQL error SqlState={SqlState} writing batch {Count} ticks (channel={Channel})",
        pgEx.SqlState, batch.Count, channelIndex);
}
catch (NpgsqlException npgEx)
{
    activity?.SetStatus(ActivityStatusCode.Error, npgEx.Message);
    _logger.LogError(npgEx,
        "Npgsql error writing batch {Count} ticks (channel={Channel})",
        batch.Count, channelIndex);
}
catch (OperationCanceledException)
{
    activity?.SetStatus(ActivityStatusCode.Error, "Cancelled");
    _logger.LogWarning("Processing batch cancelled");
    throw;
}
catch (Exception ex)
{
    activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
    _logger.LogError(ex,
        "Unexpected error processing batch of {Count} ticks (channel={Channel})",
        batch.Count, channelIndex);
}
```

#### 3.2 Add logging to RawTickRepository retry loop

**File:** [RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:368)

**Current code silently consumes retry attempts.** Add logging:

```csharp
catch (Exception ex) when (IsTransient(ex) && attempt < BulkCopyMaxRetries)
{
    attempt++;
    var delay = BulkCopyBaseDelay * (int)Math.Pow(2, attempt - 1);
    var jitter = TimeSpan.FromMilliseconds(JitterRandom.Value!.Next(100));
    _logger.LogWarning(ex,
        "BulkCopy attempt {Attempt}/{MaxRetries} failed, retrying after {Delay}ms",
        attempt, BulkCopyMaxRetries, (delay + jitter).TotalMilliseconds);
    await Task.Delay(delay + jitter, cancellationToken);
}
catch (Exception ex)
{
    _logger.LogError(ex,
        "BulkCopy failed permanently after {Attempts} attempts, batch size={Count}",
        attempt, ticks.Count);
    throw;
}
```

#### 3.3 OTel Counter for exception classification

**File:** [MarketDataTelemetry.cs](src/MarketDataCollector.Core/Telemetry/MarketDataTelemetry.cs)

Add a Counter to track exception types in real-time via Prometheus:

```csharp
public static Counter<double> ExceptionsByType { get; } =
    Meter.CreateCounter<double>(
        "exceptions_total",
        description: "Total exceptions by type");

// Usage in catch blocks:
ExceptionsByType.Add(1,
    new KeyValuePair<string, object?>("exception_type", ex.GetType().Name),
    new KeyValuePair<string, object?>("sql_state", pg?.SqlState ?? "none"));
```

**Expected effect:** Ability to identify source of 41 exceptions both in logs and Prometheus dashboard.

---

### Step 4: Gen2 GC optimization (priority: low)

#### 4.1 ArrayPool for BulkCopyAsync arrays

**File:** [RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:316)

**Change:** Use ArrayPool<T>.Shared to reduce Gen0/Gen1 allocations:

```csharp
var idsPool = ArrayPool<Guid>.Shared.Rent(count);
var tickersPool = ArrayPool<string>.Shared.Rent(count);
try { /* fill and INSERT */ }
finally
{
    ArrayPool<Guid>.Shared.Return(idsPool);
    ArrayPool<string>.Shared.Return(tickersPool);
}
```

**Note:** ArrayPool снижает Gen0/Gen1 аллокации ( fewer new[] allocations), но **не влияет на LOH** — массивы по 2,000 элементов и так ниже LOH порога.

**Expected effect:** GC allocations -60%, Gen2 GC: 4 → 1-2 in 66s.

#### 4.2 ObjectPool for batch allocations

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:385)

Use ObjectPool<List<TickData>> instead of `new List` per batch.

**Expected effect:** -50% batch list allocations.

#### 4.3 Consider SustainedLowLatency GC mode

**File:** [Program.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs)

```csharp
GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
```

**Risk:** При OOM будет hard crash вместо плавного снижения. Requires memory monitoring.

#### 4.4 Large Object Heap Compaction (NEW)

**Goal:** Directly address LOH growth (16.9 → 21.9 MB) observed in counters.

**Real LOH sources:** Npgsql internal buffers, EF Core change tracker, WebSocket/HTTP buffers — arrays larger than 85 KB allocated by these libraries.

**File:** [Program.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs) or monitoring service

**Option A — Periodic LOH compaction:**
```csharp
// Trigger once at startup, then periodically (e.g., every 5 minutes)
GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
GC.Collect(2, GCCollectionMode.Forced, blocking: false);
```

**Option B — Integrate into MonitoringService:**
```csharp
// In MonitoringService, during periodic health check:
if (GC.GetTotalMemory(forceFullCollection: false) > memoryThreshold)
{
    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
    GC.Collect(2, GCCollectionMode.Forced, blocking: false);
}
```

**Expected effect:** LOH fragmentation reduced, Gen2 GC frequency decreased. Monitor with `dotnet-counters` — LOH should stabilize.

---

## Execution Order

| Step | Priority | Complexity | Expected effect |
|------|----------|-----------|-----------------|
| 0. Batch size 2K → 5K | High | Low | -60% DB round-trips |
| 1. Retry/backoff | High | Medium | -80% outliers |
| 2.1 Timer optimization | Medium | Medium | -99% Task.Delay timers |
| 3.1-3.2 Exception logging | Medium | Low | Exception classification |
| 3.3 OTel Exception Counter | Medium | Low | Real-time exception monitoring |
| 4.1 ArrayPool | Low | Medium | -60% GC allocations |
| 4.2 ObjectPool batches | Low | Low | -50% batch list allocations |
| 2.2 Disable RuntimeInstrumentation | Low | Low | -99% OTel timers |
| 4.3 SustainedLowLatency | Low | Low | -50% GC pause |
| 4.4 LOH Compaction | Low | Low | LOH stabilization |

---

## Validation Metrics

Run after each optimization: `pwsh run_counter.ps1`

| Metric | Before | After (goal) |
|--------|--------|-------------|
| Throughput | ~17,634 ticks/sec | >20,000 ticks/sec |
| Batch write avg | 150-200 ms | 200-400 ms (larger batches) |
| Batch write p95 | 300-400 ms | <600 ms |
| Batch write max | 1,571 ms | <1,000 ms |
| DropOldest | 6,000 | <3,000 |
| Active Timers | 29,869 | <100 |
| Gen2 GC | 4 | <2 |
| GC Pause Total | 281 ms | <150 ms |
| Lock Contention | 357 | <200 |
| Channel Backlog max | 3,879 | <2,000 |
| LOH Size | 21.9 MB | <15 MB |

---

## Risks and Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Larger batch = higher latency | Medium | Medium | Monitor p99, fallback to 3000 if > 800ms |
| Retry causes delay | Low | Medium | Max 3 attempts, timeout < 5s |
| ArrayPool overhead | Low | Low | ArrayPool manages memory automatically |
| SustainedLowLatency OOM | Low | High | Memory monitoring, fallback to ServerGC |
| LOH Compaction pause | Low | Medium | Use blocking:false, monitor pause time |
| Disable RuntimeInstrumentation | Low | Low | Loss of metrics in Dashboard |

---

**Created:** 2026-07-30
**Based on:** [optimization-plan-20260730.md](plans/optimization-plan-20260730.md) (v1, audited)
**Analysis:** [counters-analysis-20260730-133957.md](plans/counters-analysis-20260730-133957.md)
**Status:** Ready for implementation
