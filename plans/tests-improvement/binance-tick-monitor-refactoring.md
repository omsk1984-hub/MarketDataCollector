# План рефакторинга: BinanceTickMonitor.cs

**Файл:** `tests/BinanceTick/BinanceTickMonitor.cs`
**Приоритет:** Средний (⚠️)

## Общая стратегия

Основные проблемы:
1. Метод `ExtractJsonValue` использует ручной поиск подстрок — не работает с числами (поле `T`: trade time) и булевыми значениями (поле `m`: isBuyerMaker)
2. Нет обработки ошибок при парсинге — при возврате "N/A" код продолжает работу с некорректными данными
3. Не добавляется ссылка на JSON-библиотеку

**Необходимое изменение:** Заменить ручной парсинг JSON на `System.Text.Json` (доступен в .NET без дополнительных NuGet-пакетов).

## Пошаговый план

### Шаг 1: Добавить using для System.Text.Json

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:1)

**Описание:** Добавить импорт пространства имён для JSON-парсинга.

```csharp
// После строки 5:
using System.Text.Json;
```

### Шаг 2: Заменить `ExtractJsonValue` на парсинг через `System.Text.Json`

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:101)

**Описание:** Метод `ExtractJsonValue` ищет подстроки вида `"key":"` и не может обработать:
- Числовые поля: `"T":1609459200000` (ищет `"T":"` — не найдёт)
- Булевы поля: `"m":true` (ищет `"m":"` — не найдёт)
- Экранированные символы в строках

**Решение:** Полностью заменить метод:

```csharp
// Удалить старый метод ExtractJsonValue (строки 101-112).

// Добавить новый:
private static string ExtractJsonValue(string json, string key)
{
    try
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        
        if (root.TryGetProperty(key, out var value))
        {
            switch (value.ValueKind)
            {
                case JsonValueKind.String:
                    return value.GetString() ?? "N/A";
                case JsonValueKind.Number:
                    return value.GetRawText();
                case JsonValueKind.True:
                    return "true";
                case JsonValueKind.False:
                    return "false";
                default:
                    return value.GetRawText();
            }
        }
    }
    catch (JsonException ex)
    {
        Console.WriteLine($"  [Ошибка парсинга JSON для ключа '{key}']: {ex.Message}");
    }
    
    return "N/A";
}
```

### Шаг 3: Обновить метод `ReceiveTicks` для обработки полей с новым парсером

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:59)

**Описание:** Логика преобразования `isBuyerMaker` изменится — теперь это будет "true"/"false", а не "N/A". 

Также поле `T` (trade time) теперь будет распознаваться как число.

**Изменение (не обязательно, но рекомендуется):** Добавить проверку `isBuyerMaker`:

```csharp
// После строки 86 (после получения isBuyerMaker):
var isBuyer = isBuyerMaker == "true";
```

### Шаг 4: Обновить вывод на консоль

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:92)

**Описание:** Обновить формат вывода с учётом корректного парсинга `isBuyerMaker`:

```csharp
// Было (строка 92):
Console.WriteLine($"[{_tickCount:D3}] {time} | {symbol} | Цена: {price} | Объем: {quantity} | Продавец инициатор: {isBuyerMaker}");

// Стало:
var isBuyer = isBuyerMaker == "true";
Console.WriteLine($"[{_tickCount:D3}] {time} | {symbol} | Цена: {price} | Объем: {quantity} | Продавец инициатор: {(isBuyer ? "Да" : "Нет")} | Время трейда (ms): {tradeTime}");
```

### Шаг 5: Добавить обработку heartbeat-сообщений (опционально)

**Описание:** Binance может отправлять периодические ping/pong или keepalive сообщения. Можно добавить их фильтрацию.

```csharp
// В методе ReceiveTicks, после получения message (после строки 78):
// Пропускаем ping-сообщения
if (message.Contains("\"e\":\"ping\"") || message.Contains("ping"))
{
    Console.WriteLine("  [Ping получен]");
    continue;
}
```

## Итоговый список изменений

| # | Файл | Изменение | Тип |
|---|------|-----------|-----|
| 1 | `tests/BinanceTick/BinanceTickMonitor.cs` | Добавить `using System.Text.Json;` | Добавление |
| 2 | `tests/BinanceTick/BinanceTickMonitor.cs` | Заменить `ExtractJsonValue` на парсинг через `JsonDocument.Parse` | Исправление |
| 3 | `tests/BinanceTick/BinanceTickMonitor.cs` | Обновить обработку `isBuyerMaker` (булево поле) | Исправление |
| 4 | `tests/BinanceTick/BinanceTickMonitor.cs` | (Опционально) Добавить фильтрацию ping-сообщений | Улучшение |
