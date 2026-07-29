# План: Замена GroupBy на раннее заполнение кеша

## Проблема

Текущий пайплайн дедупликации в [`ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:515):

```
1. GroupBy(ticker, exchange, timestamp) → intra-batch дедуп
2. Cache.Contains → cross-batch дедуп
3. BulkCopyAsync → вставка в БД
4. Cache.Add → заполнение кеша ПОСЛЕ вставки
```

**Кеш заполняется post-batch** ([строка 592-598](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:592)), поэтому **не видит intra-batch дубли** — Tick B (дубль Tick A) из того же батча не найдёт A в кеше, потому что A ещё не добавлен.

GroupBy ловит intra-batch дубли, но работает **после** накопления всего батча и требует хэширование всех ключей.

## Решение

**Раннее заполнение кеша**: добавлять тик в кеш сразу при проверке (до DB-вставки). Тогда кеш ловит **и intra-batch, и cross-batch дубли** одновременно, а GroupBy становится не нужной.

### Новый пайплайн

```
Для каждого тика в батче:
  if cache.Contains(tick) → пропустить (cached++)
  else → добавить в filteredTicks + cache.Add(tick)

BulkCopyAsync(filteredTicks) → inserted
```

### До vs После

| Аспект | До (GroupBy + post-cache) | После (ранний кеш) |
|--------|--------------------------|---------------------|
| Intra-batch дедуп | GroupBy (хэш всех ключей) | Кеш (O(1) на каждый тик) |
| Cross-batch дедуп | Кеш (post-batch) | Кеш (одновременно) |
| Кол-во итераций | 2 прохода (GroupBy + cache loop) | 1 проход |
| Кол-во добавлений в кеш | filteredTicks.Count | filteredTicks.Count (то же) |
| Эффективность кеша | ~5% дублей | ~100% дублей |

## Изменения

### 1. [`MarketDataProcessor.ProcessBatchAsync()`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:515)

**Удалить** шаг 1 (GroupBy) и шаг 1.5 (отдельный cache loop). **Заменить** на единый проход:

```csharp
// БЫЛО (строки 530-557):
var uniqueTicks = batch
    .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))
    .Select(g => g.First())
    .ToList();

List<TickData> filteredTicks;
int cachedCount = 0;
if (dedupCache != null)
{
    filteredTicks = new List<TickData>(uniqueTicks.Count);
    foreach (var t in uniqueTicks)
    {
        if (dedupCache.Contains(t.Ticker, t.Exchange, t.Timestamp))
            cachedCount++;
        else
            filteredTicks.Add(t);
    }
}
else
{
    filteredTicks = uniqueTicks;
}

// СТАЛО:
List<TickData> filteredTicks;
int cachedCount = 0;
if (dedupCache != null)
{
    filteredTicks = new List<TickData>(batch.Count);
    foreach (var t in batch)
    {
        if (dedupCache.Contains(t.Ticker, t.Exchange, t.Timestamp))
        {
            cachedCount++;
        }
        else
        {
            filteredTicks.Add(t);
            dedupCache.Add(t.Ticker, t.Exchange, t.Timestamp);
        }
    }
}
else
{
    // Кеш отключён — без GroupBy дубли попадут в БД,
    // но ON CONFLICT DO NOTHING их отсечёт.
    // Для совместимости поведения можно оставить GroupBy:
    filteredTicks = batch
        .GroupBy(t => (t.Ticker, t.Exchange, t.Timestamp))
        .Select(g => g.First())
        .ToList();
}
```

### 2. Удалить post-batch добавление в кеш

**Удалить** блок [строки 592-598](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:592):

```csharp
// УДАЛИТЬ:
if (dedupCache != null && inserted > 0)
{
    foreach (var t in filteredTicks)
    {
        dedupCache.Add(t.Ticker, t.Exchange, t.Timestamp);
    }
}
```

Кеш уже заполнен на шаге проверки — повторное добавление не нужно.

### 3. Обновить логирование

Параметр `uniq` в логе ([строка 609](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:609)) больше не применим — заменить на `filtered`:

```csharp
// БЫЛО:
_logger.LogInformation(
    "Всего: {TotalInserted} вставлено, {TotalReceived} получено (batch={BatchSize}, uniq={Unique}, cached={Cached}, вставлено={Inserted})",
    totalInserted, totalReceived, batchSize, uniqueTicks.Count, cachedCount, inserted);

// СТАЛО:
_logger.LogInformation(
    "Всего: {TotalInserted} вставлено, {TotalReceived} получено (batch={BatchSize}, filtered={Filtered}, cached={Cached}, вставлено={Inserted})",
    totalInserted, totalReceived, batchSize, filteredTicks.Count, cachedCount, inserted);
```

### 4. Обновить OpenTelemetry тег

[Строка 536](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:536):

```csharp
// БЫЛО:
activity?.SetTag("unique.count", uniqueTicks.Count);

// СТАЛО:
activity?.SetTag("filtered.count", filteredTicks.Count);
```

### 5. Обновить тесты

В [`MarketDataProcessorTests.cs`](tests/MarketDataCollector.Tests/Application/Services/MarketDataProcessorTests.cs) — проверить, что:
- Дубли внутри батча отлавливаются кешем (не GroupBy)
- Cross-batch дубли отлавликаются кешем
- При отключённом кеше (maxSize=0) дубли не фильтруются (или фильтруются GroupBy как fallback)

Тесты [`DeduplicationCacheTests.cs`](tests/MarketDataCollector.Tests/Application/Services/DeduplicationCacheTests.cs) — **без изменений** (API кеша не меняется).

## Побочные эффекты

1. **Кеш отключён (maxSize=0)**: Без GroupBy дубли попадут в БД, но `ON CONFLICT DO NOTHING` их отсечёт. `inserted` будет меньше `filteredTicks.Count`. Для совместимости можно оставить GroupBy как fallback при `dedupCache == null`.

2. **Первый батч**: Кеш пустой — все тики проходят. Группировка не нужна (все уникальны).

3. **Память**: Кеш заполняется быстрее (intra-batch дубли тоже добавляются), но `DeduplicationCache.Add()` игнорирует повторные ключи — размер не превысит maxSize.

## Метрики ожидаемого улучшения

- **cached**: с ~2 до ~33 на батч (все intra-batch дубли теперь ловятся кешем)
- **inserted**: unchanged (965 в том же примере)
- **Латентность GroupBy**: устранена (~10μs на батч — negligible)
- **Код**: проще (1 проход вместо 2)
