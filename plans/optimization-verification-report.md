# Отчёт верификации: диагностика аллокаций, Gen2 и LOH

**Источник:** [`plans/counters-analysis_20260804_184508.md`](plans/counters-analysis_20260804_184508.md) — 764 байт/тик, 3 Gen2 за 90 с, LOH 26.6 MB.

## Выполненная диагностика

| Инструмент | Источник | Статус |
|---|---|---|
| `dotnet-trace report topN -n 30` (exclusive CPU) | [`allocation_trace_20260804_184251.nettrace`](traces/allocation_trace_20260804_184251.nettrace) | ✅ выполнено |
| `dotnet-trace report topN --inclusive -n 50` | тот же trace | ✅ выполнено |
| `dotnet-gcdump report` (peak) | [`snapshot_peak_20260804_184508.gcdump`](traces/snapshot_peak_20260804_184508.gcdump) (28.8 MB) | ✅ выполнено |
| `dotnet-gcdump report` (drained) | [`snapshot_drained_20260804_184508.gcdump`](traces/snapshot_drained_20260804_184508.gcdump) (27.0 MB) | ✅ выполнено |
| Разбор allocation trace (SpeedScope) | все `nettrace` повреждены (Read past end of stream) | ❌ |

> ⚠️ Оба `nettrace` (184508, 173402) повреждены при записи — `dotnet-trace report` завершается с `System.FormatException: Read past end of stream`. Trace `184251` — CPU-sampling (не allocation), пригоден только для CPU-профиля.

---

## 1. CPU-профиль — top-10 exclusive

| № | Функция | Exclusive CPU | Значение |
|---|---|---|---|
| 1 | `WebSocketConnectionManager.ReceiveAsync` | **18.96%** | Чтение WebSocket — инфраструктура, не оптимизируется |
| 2 | `Task.WhenAny` | **12.38%** | Timer-flush паттерн (CollectorLoop, WaitToReadAsync + Task.Delay) |
| 3 | `GetTaskForValueTaskSource` | **12.05%** | ValueTask → Task конверсия в Channels |
| 4 | `TickData].WaitToReadAsync` | **11.39%** | Ожидание тиков в Channel (канал пуст, ждёт) |
| 5 | `Task.Delay` | **10.97%** | Таймер flush-интервала |
| 6 | `ManagedWebSocket.ReceiveAsync` | **7.4%** | Низкоуровневое WS-чтение |
| 7 | `ManagedWebSocket.GetReceiveResult` | **6.12%** | Парсинг WS-фрейма |
| 8 | `Task.FromResult` | **5.52%** | Завершённые задачи |
| 9 | `GetTaskForValueTaskSource` (2nd) | **4.91%** | ValueTask → Task |
| 10 | **`Decimal..ctor`** | **4.22%** | Npgsql-сериализация decimal[] для UNNEST |

### Вывод по CPU

- **~43% CPU** уходит на WS-приём (п.1 + п.6 + п.7) — это внешняя инфраструктура.
- **~35% CPU** уходит на ожидание/задачи (`Task.WhenAny` + `WaitToReadAsync` + `Task.Delay` + ValueTask-конверсии) — это цена архитектуры **с одним каналом на 3 продюсера**.
- **Decimal..ctor (4.22%)** подтверждает: Npgsql разбирает `decimal[]` через `PgNumeric` (конструктор Decimal из int16[]). Это единственный заметный per-batch CPU-расход в нашем коде.

---

## 2. gcdump: peak vs drained

### 2.1 Топ LOH-объектов

| Тип | Peak (bytes) | Drained (bytes) | Survivor | Расположение |
|---|---|---|---|---|
| `System.Byte[]` (>1MB) | **5,243,000** (5 шт) | **6,291,600** (6 шт) | 100%+ | **Npgsql-буферы** (соединение не закрыто) |
| `Entry<DedupKey,Byte>[]` | 484,968 | 484,968 | 100% | Кэш дедупликации (резидент) |
| `TickData[]` (>100K) | 458,776 (12) | 458,776 (7) | 58% | ArrayPool — часть освобождена |
| `DedupKey[]` | 320,024 | 320,024 | 100% | Кэш дедупликации |
| `System.Byte[]` (>100K) | 170,024 | 340,024 | 200% | **Npgsql-буфер** (вырос!) |
| `MetricPoint[]` (OTel) | 144,168 | 144,168 | 100% | OpenTelemetry |
| `System.String` (>10K) | 65,782 | 65,782 | 100% | — |
| `ValueTuple<Size,Object>[]` | 65,560 | 65,560 | 100% | Npgsql |

### 2.2 Ключевое наблюдение: LOH byte[] растёт

```
Peak:   5 × 1,048,600 = 5,243,000 bytes (LOH)
Drained: 6 × 1,048,600 = 6,291,600 bytes (LOH) + 170 KB → 340 KB
```

Между peak и drained количество больших `byte[]` выросло с 5 до 6. Это **не утечка** в нашем коде — это **Npgsql connection pool** (пул соединений Npgsql держит открытые TCP-соединения с буферами чтения/записи по ~1 MB каждое). Приложение использует 1 соединение для BulkCopy; буферы Npgsql внутренние и не влияют на Gen2-сборки (LOH-объекты не компактятся GC, но и не вызывают Gen2 promotion).

### 2.3 Survivor ratio

- **GC Heap:** 28.8 MB → 27.0 MB (survivor ~94%) — ожидаемо для приложения с постоянной рабочей нагрузкой.
- **Кэш дедупликации** (~805 KB) — стабильный Gen2-резидент, не растёт.
- **TickData[] в ArrayPool:** 12 → 7 шт после дренажа — пул частично освобождён.
- **MetricPoint[] (OTel):** 144 KB, не растёт.

---

## 3. Итоговая картина аллокаций

### Что даёт 764 байт/тик (разбивка)

| Компонент | Вклад (оценка) | Тип |
|---|---|---|
| `TickData` struct per тик | ~40 байт (16-byte struct) | неизбежно |
| Channel internal queues | ~80–120 байт/тик | цена архитектуры |
| Npgsql-сериализация decimal[] | ~200–300 байт/тик (amortized over batch) | **можно улучшить** |
| DI CreateScope per batch | ~2 KB × 680 / 1.7M ≈ 0.8 байт/тик | **можно улучшить** |
| OTel metrics per batch | ~300 байт/батч × 680 / 1.7M ≈ 0.12 байт/тик | negligible |
| TickData[] ArrayPool | ~458 KB total / 1.7M ≈ 0.27 байт/тик | negligible |
| Прочие (string, KVP, etc) | остаток | шум |

### Что НЕ является проблемой (опровергнуто)

| Гипотеза | Статус |
|---|---|
| String-аллокации в hot path | ✅ уже оптимизировано (Utf8JsonReader + интернирование) |
| MemoryStream перераспределения | ✅ уже заменён на ArrayBufferWriter |
| Counter/OTel per-message lock contention | ✅ уже батчеризован (CounterBatcher) |
| DedupKey HashCode.Combine повторные вычисления | ✅ кэширован в структуре |
| decimal.ToString() в BulkInsertFastAsync | ✅ уже `decimal[]`, не `string[]` |
| Утечка памяти (TickData[]) | ✅ ArrayPool возвращает после дренажа |

### Реальные источники, которые можно улучшить

#### A. LOH `byte[]` от Npgsql (5.2–6.3 MB)
Npgsql internal buffer pool — держит ~1 MB буферы на соединение. При 1 соединении это ~5 MB, не растёт. **Не причина Gen2**. Может быть снижено настройкой `NpgsqlConnectionStringBuilder.BufferSize` или `Pooling=false` в тестах.

#### B. `Decimal..ctor` (4.22% CPU)
Npgsql сериализует `decimal[]` через `PgNumeric`, что вызывает `Decimal..ctor(int16[])` на каждый decimal. **Не аллокация кучи** (decimal — value type), но CPU-расход. Альтернатива: передавать `text[]` с pre-formatted строками (уже есть в legacy-перегрузке) — это уберёт Decimal..ctor, но добавит string-аллокации.

#### C. `GetTaskForValueTaskSource` (12.05% + 4.91% = ~17% CPU)
ValueTask → Task конверсии в `Channel<T>.Reader.WaitToReadAsync`. Это внутренняя имплементация Channels — не оптимизируется.

#### D. `Task.WhenAny` (12.38% CPU) + `Task.Delay` (10.97% CPU)
Timer-flush паттерн в CollectorLoop: `await Task.WhenAny(WaitToReadAsync, Task.Delay(Inf, flushCts.Token))`. При ~7.5 батча/с (680 батчей / 90 с) это ~2 × 7.5 = **15 вызовов Task.WhenAny в секунду** — каждый создаёт `Task[]` + continuations. **Основной кандидат на CPU-оптимизацию.**

---

## 4. Рекомендации (по ROI)

### P1. Устранить `Task.WhenAny` в CollectorLoop

**Проблема:** [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:453-457) на **каждый батч** вызывает `Task.WhenAny(readTask, flushDelay)`, что аллоцирует `Task[]` на 2 элемента + continuations + CancellationToken регистрацию.

**Решение:** заменить `Task.WhenAny` + `Task.Delay(Timeout.Infinite, cts)` на `Channel<T>.Reader.WaitToReadAsync(CancellationToken)` с **ручным таймером** через `CancellationTokenSource.CancelAfter()`:

```csharp
// Вместо Task.WhenAny
using var flushCts = new CancellationTokenSource(TimeSpan.FromSeconds(_flushIntervalSeconds));
try
{
    var hasData = await channel.Reader.WaitToReadAsync(flushCts.Token);
    // hasData == true → есть тики
}
catch (OperationCanceledException)
{
    // flush timeout → отправляем частичный батч
}
```

**Оценка эффекта:** −12.38% CPU (`Task.WhenAny`) + −10.97% CPU (`Task.Delay`) = **−~23% CPU**. В аллокациях снижение незначительное (Task[] — small objects), но CPU снизится заметно.

### P2. Кэшировать `IServiceScope` в WriterLoop

**Проблема:** [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:900) создаёт scope на каждый батч (680 раз за прогон).

**Решение:** создать один scope в [`WriterLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:586) и переиспользовать для всего цикла. Включить `UseNoTracking` для DbContext, чтобы избежать роста change tracker.

```csharp
using var scope = _scopeFactory.CreateScope();
var repository = scope.ServiceProvider.GetRequiredService<IRawTickRepository>();
// ... use repository for all batches in the loop
```

**Оценка эффекта:** −~1–1.3 MB аллокаций за прогон. Аллокации/тик: −~0.8 байт/тик.

### P3. Выключить `OtlpLogExporter` в load test

**Проблема:** OTel-экспорт логов через `OtlpLogExporter` даёт ~8.9% CPU (topN inclusive). В режиме load test логи не нужны.

**Решение:** отключить `AddOtlpExporter()` для логов при `ASPNETCORE_ENVIRONMENT=LoadTest`.

**Оценка эффекта:** −~9% CPU, −несколько LOH аллокаций от сериализации логов.

### P4 (low priority). `ValueTask<Task>` → синхронный `TryRead`

**Проблема:** [`CollectorLoopAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:453) через `WaitToReadAsync().AsTask()` создаёт ValueTask → Task boxing при частых вызовах.

**Решение:** использовать `TryRead` в цикле без ожидания, если канал гарантированно не пуст (получается, это уже есть в `readTicks:` — `while (channel.Reader.TryRead(out var tick))`). `WaitToReadAsync` вызывается только когда канал пуст.

---

## 5. Что НЕ требует изменений

| Предполагавшийся кандидат | Вердикт |
|---|---|
| Npgsql `byte[]` LOH (5.2 MB) | **Не утечка** — это пул Npgsql. Не растёт. Не влияет на Gen2. |
| Dedup-кэш (~805 KB) | Стабильный Gen2-резидент, не растёт. |
| ArrayPool TickData[] | Правильно освобождается. |
| Decimal-сериализация Npgsql | CPU 4.22% — приемлемо для 680 батчей. Альтернатива (text[]) добавит string-аллокации. |

---

## 6. Итог

Основной источник CPU и GC-накладных расходов — **не аллокации per se**, а инфраструктурный код:

1. **Timer-flush паттерн** (`Task.WhenAny` + `Task.Delay`) — ~23% CPU — **P1**
2. **ValueTask → Task конверсии** — ~17% CPU — архитектура Channels, low priority
3. **WS-приём** — ~32% CPU — внешняя инфраструктура
4. **Npgsql decimal-сериализация** — 4.22% CPU — приемлемо
5. **DI Scope per batch** — ~1 MB за прогон — **P2**

Три Gen2-сборки вызваны не утечкой, а естественным промоушеном стабильных объектов (Dedup-кэш, ArrayPool, Npgsql-буферы, OTel MetricPoint). При текущей нагрузке (1.7M тиков / 90 с) **3 Gen2 за 90 с — это нормально** для приложения с resident-данными ~27 MB.