# План оптимизации: JObject → JsonDocument + InMemoryCandle struct

## Задача 1: Замена JObject.Parse на JsonDocument.Parse

**Файл:** [`src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs)

**Изменения:**
1. Заменить `using Newtonsoft.Json.Linq;` → `using System.Text.Json;`
2. Убрать `newtonsoft` dependency из `.csproj` (если не используется больше нигде)
3. Переписать `ProcessMessageAsync`:
   ```csharp
   // Было:
   var json = JObject.Parse(message);
   if (json["e"]?.ToString() == "trade") { ... }

   // Стало:
   using var doc = JsonDocument.Parse(message);
   var root = doc.RootElement;
   if (root.TryGetProperty("e", out var e) && e.GetString() == "trade") { ... }
   ```
4. Обработка ошибок: `JsonException` вместо `JsonReaderException` в тестах

**Тесты:** [`tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs)
- Заменить `using Newtonsoft.Json.Linq;` (убрать)
- Исправить assert на `System.Text.Json.JsonException` вместо `Newtonsoft.Json.JsonReaderException`

---

## Задача 2: InMemoryCandle class → struct

**Файл:** [`src/MarketDataCollector.Application/Services/TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs)

**Проблема:** `InMemoryCandle` — class (reference type), каждая свеча + строки → heap-аллокации. Ticker/Exchange дублируют `AggregatorKey`.

**Изменения:**
1. Переделать `InMemoryCandle` в `struct`:
   - Убрать поля `Ticker`, `Exchange`, `Interval` (есть в ключе + вычисляется)
   - Оставить только `decimal Open, High, Low, Close, Volume; DateTime StartTime, EndTime;`
   - `Update()` остаётся (модифицирует struct)
   - `ToAggregatedData()` убрать — будет inline с передачей ticker/exchange/interval

2. В `ProcessChannelAsync` заменить `GetOrAdd` на `AddOrUpdate`, т.к. struct возвращается по значению:
   ```csharp
   var candle = _activeCandles.AddOrUpdate(
       key,
       _ => new InMemoryCandle(tick.Price, tick.Volume, bucketStart, endTime),
       (_, existing) =>
       {
           existing.Update(tick.Price, tick.Volume);
           return existing;
       });
   ```

3. В `SaveCandlesViaDatabaseAsync` и `SaveCandlesViaKafkaAsync` — получать Ticker/Exchange/Interval из ключа + контекста:
   - `FlushCompletedCandlesAsync` уже имеет `kvp.Key` и `kvp.Value`
   - При сохранении передавать ticker/exchange/interval из `kvp.Key`

---

## Задача 3: Проверка

1. `dotnet build` — убедиться, что всё компилируется
2. `dotnet test` — запустить существующие тесты
3. Если `Newtonsoft.Json` больше нигде не используется — удалить пакет из `.csproj`
