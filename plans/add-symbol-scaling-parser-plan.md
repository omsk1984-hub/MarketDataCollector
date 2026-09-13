# План: масштабирование парсера символов Binance (клиент на инструмент)

## Контекст / проблема

В `ParseTradeMessage()` клиента [`BinanceWebSocketClient`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:123)
захардкожена цепочка из трёх инструментов:

```csharp
if (span.SequenceEqual("BTCUSDT"u8)) ticker = "BTCUSDT";
else if (span.SequenceEqual("ETHUSDT"u8)) ticker = "ETHUSDT";
else if (span.SequenceEqual("SOLUSDT"u8)) ticker = "SOLUSDT";
else ticker = reader.GetString(); // fallback
```

При сотнях инструментов это не масштабируется: код растёт линейно, а каждый
новый тикер требует правки исходника вместо конфигурации.

## Архитектурный факт (ключ к решению)

Фабрика [`CreateAllClients()`](src/MarketDataCollector.Infrastructure/Factories/WebSocketClientFactory.cs:101)
создаёт **отдельный экземпляр клиента на каждый инструмент** из секции
`Readers` конфига. Каждый клиент подписан ровно на **один** символ
([`SubscribeToTickerAsync`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:50)).
То есть каждый `BinanceWebSocketClient` уже знает свой символ — свойство
`Symbol`, унаследованное от [`BaseWebSocketClient`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:56).

**Вывод:** общий словарь-интернер не нужен. Достаточно сверять пришедший `s`
с **собственным символом экземпляра**. Это:
- масштабируется на любое число инструментов по конфигу без правок кода;
- сохраняет zero-alloc путь;
- убирает дубликаты строк на тик.

## Важный нюанс регистра

В [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:98)
символы заданы **в нижнем регистре** (`btcusdt`), а Binance присылает `s`
**в верхнем** (`BTCUSDT`). Строгое сравнение по байтам не сработает — нужна
нормализация `ToUpperInvariant` один раз в конструкторе и подготовка UTF-8:
массив байт.

## Реализация

### 1. Подготовка нормализованных данных в конструкторе

В `BinanceWebSocketClient` добавить два поля:

```csharp
private readonly string _normalizedSymbol; // Symbol.ToUpperInvariant()
private readonly byte[] _symbolUtf8;       // Encoding.UTF8.GetBytes(_normalizedSymbol)
```

Инициализировать в конструкторе после `base(...)` (там `Symbol` уже задан).

### 2. Сверка в парсере

`ParseTradeMessage()` сделать **instance-методом** (сейчас `static`), чтобы
иметь доступ к `_symbolUtf8`. Заменить хардкод-блок:

```csharp
case 's':
    if (reader.Read() && reader.TokenType == JsonTokenType.String)
    {
        // Экземпляр подписан ровно на один символ (фабрика — клиент на инструмент).
        if (reader.ValueSpan.SequenceEqual(_symbolUtf8)) ticker = _normalizedSymbol;
        else ticker = reader.GetString(); // защита от чужих символов в стриме
    }
    break;
```

Плюсы:
- совпадение по байтам `ValueSpan` — без `GetString()`/аллокаций;
- `_normalizedSymbol` уже заинтернирован/един в экземпляре — нет дубликатов;
- fallback `GetString()` сохраняет корректность для любых неожиданных сообщений.

Метод остаётся синхронным (не `async`) — ограничение ref struct `Utf8JsonReader`
не нарушается.

### 3. Тесты

Дополнить [`BinanceWebSocketClientTests.cs`](tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs):

- **Существующие** — продолжают работать: клиенты создаются с символом в верхнем
  регистре (`BTCUSDT`), совпадение по `_symbolUtf8` срабатывает.
- **Новый тест — нижний регистр из конфига:** клиент с `Symbol = "btcusdt"`,
  присылаем `s: "BTCUSDT"` → `ProcessTickAsync` получает `"BTCUSDT"`.
- **Новый тест — неизвестный/чужой символ:** клиент `BTCUSDT`, в сообщении
  `s: "XRPUSDT"` → fallback `GetString()`, тик корректно доходит до processor
  с тикером `"XRPUSDT"` (случай чужого символа в стриме; как и сейчас).

### 4. Сборка и контроль

- `dotnet build` — решение собирается.
- `dotnet test` — все тесты зелёные (включая новые).
- `dotnet format` — код соответствует стилю.

## Объём изменений

| Файл | Изменение |
|------|-----------|
| `src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs` | Поля + нормализация в конструкторе + `ParseTradeMessage` в instance + сверка с `_symbolUtf8` |
| `tests/MarketDataCollector.Tests/Infrastructure/Clients/BinanceWebSocketClientTests.cs` | +2 кейса (нижний регистр; чужой символ) |

Никаких изменений в фабрике, конфиге и DI не требуется.