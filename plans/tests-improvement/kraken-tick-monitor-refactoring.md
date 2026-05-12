# План рефакторинга: KrakenTickMonitor.cs

**Файл:** `tests/KrakenTick/KrakenTickMonitor.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. Метод `ExtractJsonValue` использует ручной поиск подстрок — для Kraken это критичнее, так как JSON содержит вложенные структуры (`data[0].symbol`, `data[0].price`)
2. Ручной парсинг находит первое вхождение ключа, которое может быть в системном сообщении, а не в данных
3. Формат подписки может отличаться в зависимости от версии API Kraken

**Необходимое изменение:** Заменить ручной парсинг JSON на `System.Text.Json` и корректно обрабатывать вложенные структуры данных.

## Пошаговый план

### Шаг 1: Добавить using для System.Text.Json

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:1)

**Описание:** Добавить импорт пространства имён для JSON-парсинга.

```csharp
// После строки 5:
using System.Text.Json;
```

### Шаг 2: Заменить `ExtractJsonValue` на парсинг через `System.Text.Json`

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:118)

**Описание:** Текущий `ExtractJsonValue` пытается найти ключ во всём JSON и берёт первое вхождение. Для Kraken данные находятся в массиве `data`, и ключи (например, `symbol`) могут встречаться в разных контекстах.

Формат сообщения Kraken (типичный):
```json
{
    "channel": "trade",
    "type": "snapshot",
    "data": [
        {
            "symbol": "BTC/USD",
            "price": "50000.0",
            "qty": "0.1",
            "side": "sell",
            "trade_id": 12345,
            "timestamp": "2024-01-01T00:00:00.000000Z"
        }
    ]
}
```

**Решение:** Заменить `ExtractJsonValue` на парсинг первого элемента массива `data`:

```csharp
// Удалить старый метод ExtractJsonValue (строки 118-139).

// Добавить новый метод ParseFirstTickData, который парсит первый элемент из data[]:
private static (string Symbol, string Price, string Quantity, string Side, string TradeId, string Timestamp) ParseFirstTickData(string json)
{
    try
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        
        // Получаем массив data
        if (root.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array && dataArray.GetArrayLength() > 0)
        {
            var firstTick = dataArray[0];
            
            var symbol = GetStringOrRaw(firstTick, "symbol");
            var price = GetStringOrRaw(firstTick, "price");
            var quantity = GetStringOrRaw(firstTick, "qty");
            var side = GetStringOrRaw(firstTick, "side");
            var tradeId = GetStringOrRaw(firstTick, "trade_id");
            var timestamp = GetStringOrRaw(firstTick, "timestamp");
            
            return (symbol, price, quantity, side, tradeId, timestamp);
        }
    }
    catch (JsonException ex)
    {
        Console.WriteLine($"  [Ошибка парсинга JSON]: {ex.Message}");
    }
    
    return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A");
}

private static string GetStringOrRaw(JsonElement element, string key)
{
    if (element.TryGetProperty(key, out var value))
    {
        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString() ?? "N/A",
            JsonValueKind.Number => value.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => value.GetRawText()
        };
    }
    return "N/A";
}
```

### Шаг 3: Обновить метод `ReceiveTicks` для использования нового парсера

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:65)

**Описание:** Заменить вызовы `ExtractJsonValue` на структурированный парсинг.

```csharp
// В методе ReceiveTicks, внутри блока if (строки 87-100):

// Было:
var symbol = ExtractJsonValue(message, "symbol");
var price = ExtractJsonValue(message, "price");
var quantity = ExtractJsonValue(message, "qty");
var side = ExtractJsonValue(message, "side");
var tradeId = ExtractJsonValue(message, "trade_id");
var timestamp = ExtractJsonValue(message, "timestamp");

Console.WriteLine($"[{_tickCount:D3}] {timestamp} | {symbol} | Цена: {price} | Объем: {quantity} | Сторона: {side} | ID: {tradeId}");

// Стало:
var (symbol, price, quantity, side, tradeId, timestamp) = ParseFirstTickData(message);

Console.WriteLine($"[{_tickCount:D3}] {timestamp} | {symbol} | Цена: {price} | Объем: {quantity} | Сторона: {side} | ID: {tradeId}");
```

### Шаг 4: Обновить фильтрацию системных сообщений

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:87)

**Описание:** Текущая проверка `message.Contains("\"channel\":\"trade\"") && message.Contains("\"data\"")` может срабатывать на неверных сообщениях. С новым парсером можно использовать структурированную проверку.

```csharp
// Вместо (строки 87-88):
if (message.Contains("\"channel\":\"trade\"") && message.Contains("\"data\""))

// Использовать (добавить метод-хелпер):
private static bool IsTradeMessage(string json)
{
    try
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        
        // Проверяем channel == "trade"
        if (!root.TryGetProperty("channel", out var channel) || channel.GetString() != "trade")
            return false;
            
        // Проверяем наличие data с типом array
        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
            return false;
            
        return data.GetArrayLength() > 0;
    }
    catch
    {
        return false;
    }
}
```

### Шаг 5 (Опционально): Добавить обработку heartbeat-сообщений Kraken

**Описание:** Kraken периодически отправляет `{"event":"heartbeat"}`. Можно добавить их отображение.

```csharp
// В методе ReceiveTicks, после получения message (после строки 84):
if (message.Contains("\"event\":\"heartbeat\"") || message.Contains("heartbeat"))
{
    Console.WriteLine("  [Heartbeat]");
    continue;
}
```

### Шаг 6 (Опционально): Проверить актуальность формата подписки Kraken v2

**Описание:** Формат сообщения подписки (строка 14) соответствует Kraken Websocket API v2:
```json
{"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
```

Если API v2 изменился, нужно обновить сообщение подписки. Рекомендуется проверить актуальную документацию Kraken.

## Итоговый список изменений

| # | Файл | Изменение | Тип |
|---|------|-----------|-----|
| 1 | `tests/KrakenTick/KrakenTickMonitor.cs` | Добавить `using System.Text.Json;` | Добавление |
| 2 | `tests/KrakenTick/KrakenTickMonitor.cs` | Заменить `ExtractJsonValue` на `ParseFirstTickData` + `GetStringOrRaw` | Исправление |
| 3 | `tests/KrakenTick/KrakenTickMonitor.cs` | Обновить `ReceiveTicks` для использования `ParseFirstTickData` | Исправление |
| 4 | `tests/KrakenTick/KrakenTickMonitor.cs` | Заменить фильтрацию `Contains` на `IsTradeMessage` | Улучшение |
| 5 | `tests/KrakenTick/KrakenTickMonitor.cs` | (Опционально) Добавить фильтрацию heartbeat | Улучшение |
| 6 | `tests/KrakenTick/KrakenTickMonitor.cs` | (Опционально) Проверить формат подписки Kraken v2 | Исследование |
