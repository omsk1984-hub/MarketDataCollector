# Optimization Plan: MarketDataCollector

**Based on:** counters-analysis-20260730-133957.md (run 2: 1.2M ticks, ~18K ticks/sec)
**Goal:** increase write throughput, reduce latency, eliminate losses
**Current metrics:** ~17,634 ticks/sec, batch write 150-200ms (outliers up to 1.57s), 6K DropOldest, 29,869 timers, 41 exceptions, 4 Gen2 GC in 66 sec

---

## Current Problems Analysis

### Problem 1: Retry/backoff only for deadlock (1 attempt)

**Source:** RawTickRepository.BulkCopyAsync() - retry loop with 1 attempt

**Current behavior:**
- Only 1 retry for deadlock (40P01) and NpgsqlException
- Fixed delay 200ms without exponential backoff
- Outliers up to 1.57s not handled

**Solution:** Add retry with exponential backoff (3-5 attempts, 100ms to 1600ms) for transient errors.

### Problem 2: Abnormally many timers (29,869)

**Timer sources in production:**

| Source | File | Count | Type |
|--------|------|-------|------|
| Task.Delay flush timer | MarketDataProcessor.cs:411 | 3 | Internal timer |
| MonitoringService._statusTimer | MonitoringService.cs:38 | 1 | System.Threading.Timer |
| TickAggregator._flushTimer | TickAggregator.cs:156 | 0-1 | System.Threading.Timer |
| OTel RuntimeInstrumentation | Program.cs | MANY | ETW/EventSource |

**Solution:**
1. Replace Task.Delay flush timer with System.Threading.Timer reuse
2. Consider disabling AddRuntimeInstrumentation()

### Problem 3: 41 exceptions in 66 sec

**Possible sources:** Npgsql timeout, ChannelClosedException, Connection broken, TaskCanceledException

**Solution:** Add structured logging for exception classification by type.

### Problem 4: Gen2 GC 4 times in 66 sec (281 ms pauses)

**Source:** Allocation rate ~44.5 MB/sec. Arrays Guid[], string[], decimal[] in BulkCopyAsync().

**Solution:** Use ArrayPool for arrays in BulkCopyAsync().

---

## Optimization Plan

### Step 1: Retry/backoff for batch write (priority: high)

**Goal:** Eliminate outliers up to 1.57s, increase write reliability.

**File:** [RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:359)

**Change:** 3 attempts with exponential backoff + jitter:

`csharp
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
            "BulkCopy attempt {Attempt}/{MaxRetries} failed, retrying after {Delay}ms",
            attempt, BulkCopyMaxRetries, (delay + jitter).TotalMilliseconds);
        await Task.Delay(delay + jitter, cancellationToken);
    }
}

private static bool IsTransient(Exception ex) =>
    (ex is PostgresException pg && pg.SqlState is "40P01" or "57014" or "08006" or "08001" or "08003")
    || ex is NpgsqlException
    || ex is TimeoutException;
`

**Expected effect:** Outliers > 1s reduced by ~80%.

---

### Step 2: Timer optimization (priority: medium)

**Goal:** Reduce timer count from 29,869 to < 100.

#### 2.1 Replace Task.Delay flush timer with System.Threading.Timer

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:408)

**Change:** Use System.Threading.Timer with reuse instead of Task.Delay per iteration:

`csharp
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
`

**Expected effect:** Timers: ~66 in 66s -> ~3 (reuse).

#### 2.2 Consider disabling AddRuntimeInstrumentation

**File:** [Program.cs](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Program.cs)

If OTel GC/ThreadPool metrics are not critical, disabling reduces timers from ~29,000 to < 100.

---

### Step 3: Exception investigation (priority: medium)

**Goal:** Classify 41 exceptions, identify source.

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:636)

**Change:** Add exception classification:

`csharp
catch (PostgresException pgEx)
{
    _logger.LogError(pgEx,
        "PostgreSQL error {SqlState} writing batch {Count} ticks (channel={Channel})",
        pgEx.SqlState, batch.Count, channelIndex);
}
catch (NpgsqlException npgEx)
{
    _logger.LogError(npgEx,
        "Npgsql error writing batch {Count} ticks (channel={Channel})",
        batch.Count, channelIndex);
}
catch (Exception ex)
{
    _logger.LogError(ex,
        "Unexpected error processing batch of {Count} ticks (channel={Channel})",
        batch.Count, channelIndex);
}
`

**Expected effect:** Ability to identify source of 41 exceptions.

---

### Step 4: Gen2 GC monitoring (priority: low)

#### 4.1 ArrayPool for BulkCopyAsync arrays

**File:** [RawTickRepository.cs](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:316)

**Change:** Use ArrayPool<T>.Shared:

`csharp
var idsPool = ArrayPool<Guid>.Shared.Rent(count);
var tickersPool = ArrayPool<string>.Shared.Rent(count);
try { /* fill and INSERT */ }
finally
{
    ArrayPool<Guid>.Shared.Return(idsPool);
    ArrayPool<string>.Shared.Return(tickersPool);
}
`

**Expected effect:** GC allocations -60%, Gen2 GC: 4 -> 1-2 in 66s.

#### 4.2 ObjectPool for batch allocations

**File:** [MarketDataProcessor.cs](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:385)

Use ObjectPool<List<TickData>> instead of 
ew List per batch.
**Expected effect:** -50% batch list allocations.

#### 4.3 Consider SustainedLowLatency GC mode

`csharp
GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
`

---

## Execution Order

| Step | Priority | Complexity | Time | Expected effect |
|------|----------|-----------|------|-----------------|
| 1. Retry/backoff | High | Medium | 1 hour | -80% outliers |
| 2.1 Timer optimization | Medium | Medium | 2 hours | -99% timers |
| 3. Exception logging | Medium | Low | 30 min | Exception classification |
| 4.1 ArrayPool | Low | Medium | 1.5 hours | -60% GC allocations |
| 4.2 ObjectPool batches | Low | Low | 30 min | -50% batch allocations |
| 2.2 Disable RuntimeInstrumentation | Low | Low | 15 min | -99% timers |
| 4.3 GC SustainedLowLatency | Low | Low | 15 min | -50% GC pause |

---

## Validation Metrics

Run after each optimization: pwsh run_counter.ps1

| Metric | Before | After (goal) |
|--------|--------|-------------|
| Throughput | ~17,634 ticks/sec | >20,000 ticks/sec |
| Batch write avg | 150-200 ms | 150-250 ms |
| Batch write p95 | 300-400 ms | <500 ms |
| Batch write max | 1,571 ms | <800 ms |
| DropOldest | 6,000 | <3,000 |
| Active Timers | 29,869 | <100 |
| Gen2 GC | 4 | <2 |
| GC Pause Total | 281 ms | <150 ms |
| Lock Contention | 357 | <200 |
| Channel Backlog max | 3,879 | <2,000 |

---

## Risks and Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Retry causes delay | Low | Medium | Max 3 attempts, timeout < 5s |
| ArrayPool overhead | Low | Low | ArrayPool manages memory automatically |
| GC SustainedLowLatency OOM | Low | High | Memory monitoring, fallback |
| Disable RuntimeInstrumentation | Low | Low | Loss of metrics in Dashboard |

---

**Created:** 2026-07-30
**Analysis:** [plans/counters-analysis-20260730-133957.md](plans/counters-analysis-20260730-133957.md)
**Status:** Ready for implementation