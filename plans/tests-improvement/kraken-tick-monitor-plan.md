# План улучшения: KrakenTickMonitor.cs

**Файл:** `tests/KrakenTick/KrakenTickMonitor.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Примитивный парсинг JSON

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:118)

**Описание:** Аналогично BinanceTickMonitor, метод `ExtractJsonValue` использует ручной поиск подстрок. Для Kraken формат сообщений сложнее — данные находятся во вложенных структурах. Ручной парсинг может найти первое вхождение ключа, которое может быть не в том контексте.

**Необходимо:** Использовать JSON-парсер.

### Проблема 2: Неверный формат сообщения подписки для Kraken v2

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:14)

**Описание:** Тест использует URL `wss://ws.kraken.com/v2` и сообщение подписки:
```json
{"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
```

Формат сообщения может отличаться в зависимости от версии API Kraken. В Kraken Websocket API v2 формат подписки может требовать другие поля. Необходимо проверить актуальность формата.

**Необходимо:** Проверить актуальный формат подписки Kraken Websocket API v2.

### Проблема 3: Парсинг вложенных данных

**Файл:** [`tests/KrakenTick/KrakenTickMonitor.cs`](tests/KrakenTick/KrakenTickMonitor.cs:87)

**Описание:** Сообщения Kraken содержат данные в массиве `data`, где каждый элемент — объект с полями `symbol`, `price`, `qty`, `side`, `trade_id`, `timestamp`. Ручной парсинг через `ExtractJsonValue` найдёт первое вхождение ключа в сообщении, которое может быть в системном сообщении, а не в данных.

**Необходимо:** Использовать JSON-парсер для корректного извлечения данных из вложенных структур.

### Рекомендации:

1. **Заменить ручной парсинг JSON** на `Newtonsoft.Json` или `System.Text.Json`.
2. **Добавить NuGet-пакет** `Newtonsoft.Json` в `KrakenTickMonitor.csproj`.
3. **Проверить актуальность формата подписки** Kraken Websocket API v2.
4. **Добавить обработку ошибок** при парсинге.
5. **Добавить поддержку heartbeat-сообщений** Kraken (периодические `{"event":"heartbeat"}`).