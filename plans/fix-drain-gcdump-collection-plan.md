# План: исправление сбора drained gcdump

## 1. Проблема

В прогоне 20260912_092741 второй gcdump (drained) снят **в фазе роста нагрузки**, а не после дренажа:
- GC Heap bytes выросли на 13% (31.2 → 35.2 MB) между peak и drained
- `TickData[]` отсутствовал в peak, но появился в drained (1.8 MB)
- В смежном прогоне 092421 куча стабильна (+1.7%), аномалии нет

**Корневая причина:** [`start_loadtest.ps1`](start_loadtest.ps1:292) запускает Profiler (шаг 5/7) **до** ожидания завершения генерации FakeServer (шаг 6/7). Profiler делает drain (шаг 9) с `DrainWaitSec=30`, но в этот момент FakeServer всё ещё генерирует тики (1.7M / 25K RPS ≈ 68 с генерации). Drain-фаза начинается на ~55-й секунде прогона, а генерация заканчивается на ~68-й. Таким образом, на протяжении drain-фазы тики продолжают поступать в канал, и он не может опустошиться.

**Вторичная причина:** [`DrainWaiter.cs`](tools/Profiler/Core/Waiters/DrainWaiter.cs:46) считает дренаж завершённым при **первом же** обнулении backlog'а (`total <= 0`), даже если это кратковременный спад между пачками тиков, а не устойчивый 0.

---

## 2. Архитектурное решение

### Подход: двухфазный сбор с сигналом от оркестратора

```
┌─────────────────────────────────────────────────────────────┐
│                     Profiler (1 процесс)                    │
├─────────────────────────────────────────────────────────────┤
│  Phase 1: trace + counters + peak gcdump                    │
│  Phase 2: ожидание HTTP-сигнала → drain → second gcdump    │
└─────────────────────────────────────────────────────────────┘
         ▲ HTTP POST                              │
         │ /trigger-drained-collect               │
    ┌────┴────────────────────────────┐            │
    │     start_loadtest.ps1          │            │
    │  1. Запустить Profiler          │            │
    │  2. Дождаться генерации         │────────────┘
    │  3. POST /trigger-drained-collect│
    │  4. Дождаться завершения       │
    └────────────────────────────────┘
```

### Ключевые изменения

1. **ProfilerOrchestrator** — двухфазный сбор: Phase 1 (trace+peak), затем ожидание внешнего триггера, затем Phase 2 (drain+drained gcdump)
2. **ProfilerHttpServer** — новый endpoint `/trigger-drained-collect`
3. **DrainWaiter** — ужесточение: N последовательных чтений backlog=0 перед declare drain complete
4. **start_loadtest.ps1** — вставка вызова `/trigger-drained-collect` после завершения генерации
5. **ProfilerOptions** — новый параметр `DrainConsecutiveZeroChecks`
6. **run_all_profiler.ps1** — проксировать параметры

---

## 3. Детальные изменения

### 3.1 ProfilerOptions — новые поля

| Поле | Тип | По умолч. | Описание |
|------|-----|-----------|----------|
| `DrainConsecutiveZeroChecks` | `int` | `3` | Сколько последовательных опросов с backlog=0 нужно для объявления дренажа |
| `DrainPollIntervalSec` | `int` | `2` | Интервал между опросами /metrics (был `PollIntervalSec=2`, вынести в Options) |

### 3.2 DrainWaiter — ужесточение проверки

**Файл:** [`tools/Profiler/Core/Waiters/DrainWaiter.cs`](tools/Profiler/Core/Waiters/DrainWaiter.cs)

**Изменения:**
1. Вынести `PollIntervalSec` в `ProfilerOptions.DrainPollIntervalSec`
2. Добавить счётчик `_consecutiveZeroCount`. При каждом чтении backlog:
   - Если `total <= 0` — инкремент `_consecutiveZeroCount`
   - Если `total > 0` — сброс `_consecutiveZeroCount = 0`
   - Если `_consecutiveZeroCount >= _options.DrainConsecutiveZeroChecks` — drain complete
3. Логировать `consecutiveZeroCount` на каждом опросе (Debug)
4. При истечении таймаута — логировать текущий fill_level (для диагностики), а не просто "Таймаут истёк"

### 3.3 ProfilerOrchestrator — двухфазный сбор

**Файл:** [`tools/Profiler/Services/ProfilerOrchestrator.cs`](tools/Profiler/Services/ProfilerOrchestrator.cs)

**Изменения:**
1. Метод `RunAllAsync` теперь останавливается **после остановки trace** (шаг 8), НЕ делает drain/second gcdump
2. Вместо этого создаётся `TaskCompletionSource<bool> _drainTrigger` — сигнал от HTTP-сервера
3. Создаётся новый публичный метод `RunDrainPhaseAsync(int processId, string drainedGcDumpPath)`:
   - Ожидает `_drainTrigger.Task` (сигнал от /trigger-drained-collect)
   - Выполняет drain wait + second gcdump
4. `RunAllAsync` вызывается сначала, затем `RunDrainPhaseAsync` после сигнала

ИЛИ (проще): **единый `RunAllAsync` с ожиданием внутри:**

```csharp
// В ProfilerOrchestrator:
private readonly TaskCompletionSource<bool> _drainSignal = new();

public async Task<int> RunAllAsync(CancellationToken ct)
{
    // ... шаги 1-8 (trace + counters + peak gcdump)
    
    // Шаг 8.5: ожидание внешнего сигнала дренажа
    _metrics.SetCurrentStep("8.5. Ожидание сигнала дренажа");
    _ui.SectionHeader("8.5. Ожидание сигнала дренажа от оркестратора");
    
    using var signalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    signalCts.CancelAfter(TimeSpan.FromSeconds(_options.DrainSignalTimeoutSec)); // fallback timeout
    
    try 
    {
        await _drainSignal.Task.WaitAsync(signalCts.Token);
        _ui.Ok("Получен сигнал дренажа — запуск второй фазы.");
    }
    catch (OperationCanceledException)
    {
        _ui.Warn("Таймаут ожидания сигнала дренажа — продолжение по таймауту.");
    }
    
    // Шаг 9: дренаж + второй gcdump (как сейчас)
    // ...
}

public void SignalDrainReady()
{
    _drainSignal.TrySetResult(true);
}
```

### 3.4 ProfilerHttpServer — новый endpoint

**Файл:** [`tools/Profiler/Services/ProfilerHttpServer.cs`](tools/Profiler/Services/ProfilerHttpServer.cs)

**Изменения:**
1. Добавить новый endpoint `POST /trigger-drained-collect`:
   ```csharp
   app.MapPost("/trigger-drained-collect", () =>
   {
       _orchestrator.SignalDrainReady();
       return Results.Accepted("/health", new { status = "drain_signaled" });
   });
   ```
2. Добавить endpoint `GET /status` (для диагностики):
   ```csharp
   app.MapGet("/status", () => new {
       phase = _drainSignal.Task.IsCompleted ? "draining" : "awaiting-drain-signal",
       // ...
   });
   ```

### 3.5 start_loadtest.ps1 — переупорядочивание шагов

**Файл:** [`start_loadtest.ps1`](start_loadtest.ps1)

**Изменение:** После завершения Profiler (Phase 1) и завершения генерации — отправить HTTP-сигнал Profiler'у.

**Блок-схема нового порядка:**

```
Шаг 5/7: Запуск Profiler
    ↓
  Profiler Phase 1: trace + counters + peak gcdump
    ↓
  Profiler завершает Phase 1, но **процесс продолжает работать**
  (ждёт сигнала на порту :5100)
    ↓
Шаг 6/7: Ожидание завершения генерации FakeServer
    ↓
  POST http://localhost:5100/trigger-drained-collect
    ↓
  Profiler Phase 2: drain wait → second gcdump
    ↓
  Profiler завершается (exit code)
    ↓
Шаг 7/7: Остановка Worker
```

**Конкретные изменения кода:**

После строки [`& "$root/run_all_profiler.ps1" ...`](start_loadtest.ps1:292):
```powershell
Write-Step "[5b/7] Ожидание Profiler Phase 1 (trace + peak)"
# Profiler теперь не делает drain — он ждёт сигнала на :5100
# Ждём, пока Profiler HTTP-сервер не покажет phase=awaiting-drain-signal
Wait-Http "http://localhost:5100/status-json?phase=awaiting" 60 "Profiler Phase 1"
```

После завершения генерации (шаг 6/7), перед остановкой Worker:
```powershell
# ============================================================
# Сигнал Profiler'у: фаза дренажа
# ============================================================
Write-Step "[6b/7] Сигнал Profiler'у на сбор drained gcdump"
try {
    $resp = Invoke-RestMethod -Uri "http://localhost:5100/trigger-drained-collect" -Method Post -TimeoutSec 10
    Write-Host "  /trigger-drained-collect -> $($resp.status)" -ForegroundColor Green
}
catch {
    Write-Host "  Ошибка POST /trigger-drained-collect: $($_.Exception.Message)" -ForegroundColor Yellow
}

# Ждём завершения Profiler
Wait-ProcessExit $profilerProc 120 "Profiler (drain phase)"
```

### 3.6 ProfilerOptions — новый параметр DrainSignalTimeoutSec

| Поле | Тип | По умолч. | Описание |
|------|-----|-----------|----------|
| `DrainSignalTimeoutSec` | `int` | `180` | Сколько секунд Profiler ждёт внешнего сигнала дренажа (fallback) |

### 3.7 CLI — новые аргументы

**Файл:** [`tools/Profiler/Cli/CommandLineParser.cs`](tools/Profiler/Cli/CommandLineParser.cs)

Добавить парсинг `--drain-consecutive-zero-checks`, `--drain-poll-interval-sec`, `--drain-signal-timeout-sec`.

### 3.8 run_all_profiler.ps1 — прокси новые параметры

**Файл:** [`run_all_profiler.ps1`](run_all_profiler.ps1)

Добавить параметры:
- `-DrainConsecutiveZeroChecks` (int, default 3)
- `-DrainPollIntervalSec` (int, default 2)
- Прокинуть в вызов `MarketDataCollector.Profiler.exe`

---

## 4. Диаграмма последовательности (Sequence Diagram)

```mermaid
sequenceDiagram
    participant O as start_loadtest.ps1
    participant P as Profiler
    participant F as FakeTickServer
    participant W as Worker

    rect rgb(200, 230, 200)
    Note over O,P,F,W: Phase 1: Trace + Counters + Peak GcDump
    O->>P: Запуск run_all_profiler.ps1
    P->>W: health-check
    P->>W: start dotnet-trace
    P->>W: start counters collection
    P->>W: wait peak (50s)
    P->>W: collect peak gcdump
    P->>W: stop trace
    P->>P: Открыть HTTP :5100
    P-->>O: Phase 1 завершена, ожидание сигнала
    end

    rect rgb(230, 230, 200)
    Note over O,F,W: Генерация тиков
    F->>W: генерация тиков (1.7M)
    O->>F: Wait-GenerationComplete (polling /health)
    F-->>O: isLimitReached=true
    end

    rect rgb(200, 200, 230)
    Note over O,P,W: Phase 2: Drain + Drained GcDump
    O->>P: POST /trigger-drained-collect
    P->>W: drain wait (backlog=0, 3 consecutive zero checks)
    P->>W: collect drained gcdump
    P-->>O: Profiler exit (complete)
    end

    O->>W: POST /shutdown
```

---

## 5. Обратная совместимость

- Старый `DrainWaitSec=30` остаётся как fallback timeout для второй фазы.
- Если Profiler запущен **без HTTP-сервера** (`--http-enabled=false`), то `_drainSignal` никогда не получит сигнал, и сработает `DrainSignalTimeoutSec` — поведение будет как сегодня (дождался таймаута → пошёл дренаж с обратным отсчётом). Это сохраняет совместимость со старыми скриптами.
- Если `--http-enabled=true` (по умолчанию), то Profiler ждёт внешнего сигнала.

**Фолбэк-таймаут:** `DrainSignalTimeoutSec=180` (3 минуты) — если оркестратор упал и не послал сигнал, Profiler не зависнет навсегда.

---

## 6. Файлы для изменения

| # | Файл | Суть изменения |
|---|------|----------------|
| 1 | [`tools/Profiler/Options/ProfilerOptions.cs`](tools/Profiler/Options/ProfilerOptions.cs) | +`DrainConsecutiveZeroChecks`, `DrainPollIntervalSec`, `DrainSignalTimeoutSec` |
| 2 | [`tools/Profiler/Core/Interfaces/IDrainWaiter.cs`](tools/Profiler/Core/Interfaces/IDrainWaiter.cs) | Обновить контракт (возможно, `PassOptions`) |
| 3 | [`tools/Profiler/Core/Waiters/DrainWaiter.cs`](tools/Profiler/Core/Waiters/DrainWaiter.cs) | N-последовательных проверок backlog=0; логирование |
| 4 | [`tools/Profiler/Services/ProfilerOrchestrator.cs`](tools/Profiler/Services/ProfilerOrchestrator.cs) | Разделение на 2 фазы; `_drainSignal` TCS; `SignalDrainReady()` |
| 5 | [`tools/Profiler/Services/ProfilerHttpServer.cs`](tools/Profiler/Services/ProfilerHttpServer.cs) | +endpoint `/trigger-drained-collect` |
| 6 | [`tools/Profiler/Cli/CommandLineParser.cs`](tools/Profiler/Cli/CommandLineParser.cs) | Парсинг новых параметров |
| 7 | [`tools/Profiler/Cli/DiContainer.cs`](tools/Profiler/Cli/DiContainer.cs) | Регистрация новой зависимости (если нужна) |
| 8 | [`run_all_profiler.ps1`](run_all_profiler.ps1) | Новые параметры, прокси в Profiler |
| 9 | [`start_loadtest.ps1`](start_loadtest.ps1) | Сигнал `/trigger-drained-collect` после генерации |

---

## 7. Критерии успеха

1. **Повторный прогон** `start_loadtest.ps1` (1.7M ticks, 25K RPS):
   - Peak gcdump: куча ~30-40 MB, `TickData[]` канала ≤ 7.34 MB
   - Drained gcdump: куча **меньше или равна peak** (не больше), `TickData[]` канала = 0 (канал дренирован)
   - GC Heap bytes drained ≤ GC Heap bytes peak (survivor ratio ≤ 100%)

2. **Логи:** в логах Profiler:
   - `"Drain signal received"` или `"Drain signal timeout"` (в зависимости от режима)
   - `"Consecutive zero checks: 3/3"` перед second gcdump
   - Если таймаут — `"Drain timeout expired, fill_level = <value>"`

3. **Регрессии:** существующие метрики (counters CSV, trace, peak gcdump) сохраняют структуру и формат.

---

## 8. Риски

| Риск | Вероятность | Смягчение |
|------|:----------:|-----------|
| Profiler зависнет в ожидании сигнала | Низкая | `DrainSignalTimeoutSec=180` — автоматический переход ко второй фазе |
| HTTP-сервер Profiler недоступен (:5100) | Низкая | Фолбэк-таймаут сработает, как при старом поведении |
| Двойной запуск Profiler | Низкая | Скрипт проверяет занятость порта (есть в health-чеке) |
| Worker завершился до второго gcdump | Средняя | Profiler проверяет PID перед gcdump (`ProcessFinder`) — если worker мёртв, gcdump не берётся |

---

*План составлен на основе [`plans/gcdump-analysis_20260912_092741.md`](plans/gcdump-analysis_20260912_092741.md) — рекомендация п.6.2: "увеличить DrainWaitSec или убедиться, что дренаж завершён (backlog=0) перед снятием второго gcdump".*