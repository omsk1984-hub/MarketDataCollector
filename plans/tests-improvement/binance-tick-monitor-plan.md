# План улучшения: BinanceTickMonitor.cs

**Файл:** `tests/BinanceTick/BinanceTickMonitor.cs`

## Результат проверки: ⚠️ Есть проблемы

### Проблема 1: Примитивный парсинг JSON

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:101)

**Описание:** Метод `ExtractJsonValue` использует ручной поиск подстрок для парсинга JSON. Это ненадёжно:
- Не работает с экранированными символами
- Не работает с числами без кавычек (например, `"T": 1609459200000`)
- Не работает с вложенными объектами
- Может найти значение не в том контексте

В реальном JSON от Binance поле `T` (trade time) — это число, а не строка. Метод ищет `"T":"` и не найдёт его, так как в JSON будет `"T":1609459200000`. В результате `tradeTime` будет "N/A".

**Необходимо:** Использовать `Newtonsoft.Json` или `System.Text.Json` для парсинга.

### Проблема 2: Поле `m` (isBuyerMaker) — булево значение

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:86)

**Описание:** Поле `m` в JSON от Binance — это булево значение (`true`/`false`), а не строка. Метод `ExtractJsonValue` ищет `"m":"`, но в JSON будет `"m":true`. Парсинг не сработает.

**Необходимо:** Использовать JSON-парсер для корректного извлечения булевых значений.

### Проблема 3: Нет обработки ошибок при парсинге

**Файл:** [`tests/BinanceTick/BinanceTickMonitor.cs`](tests/BinanceTick/BinanceTickMonitor.cs:88)

**Описание:** Если `ExtractJsonValue` возвращает "N/A", то `long.TryParse("N/A", out var t)` вернёт `false`, и время будет "N/A". Ошибка не логируется.

**Необходимо:** Добавить логирование ошибок парсинга.

### Рекомендации:

1. **Заменить ручной парсинг JSON** на `Newtonsoft.Json` или `System.Text.Json`.
2. **Добавить NuGet-пакет** `Newtonsoft.Json` в `BinanceTickMonitor.csproj`.
3. **Добавить обработку ошибок** при парсинге.
4. **Добавить поддержку булевых и числовых полей**.