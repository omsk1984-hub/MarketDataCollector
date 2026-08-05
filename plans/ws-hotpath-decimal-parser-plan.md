# План: hot-path WebSocket оптимизация (статус + оставшаяся доработка)

**Источник:** [`plans/loh-gen2-channel-improvement-plan.md`](plans/loh-gen2-channel-improvement-plan.md) п. P3.5 и [`plans/allocation-reduction-hotpath-plan.md`](plans/allocation-reduction-hotpath-plan.md).
**Цель:** довести hot-path от WebSocket до Channel до zero-alloc.

---

## 1. Текущий статус кода (проверено по исходникам)

| Шаг из плана | Файл | Статус |
|---|---|---|
| Проброс `ReadOnlyMemory<byte>` вместо `string` | [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:38) | ✅ **Реализовано** — `Func<ReadOnlyMemory<byte>, Task>`, сырые байты без `Encoding.UTF8.GetString` |
| Событие `MessageReceived` | [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:59) | ✅ **Реализовано** — `EventHandler<ReadOnlyMemory<byte>>` |
| Сигнатуры `ProcessMessageAsync` / `OnMessageReceived` | [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:274,398) | ✅ **Реализовано** — оба принимают `ReadOnlyMemory<byte>` |
| Ручной `Utf8JsonReader` (0 DOM-аллокаций) | [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:124) `ParseTradeMessage` | ✅ **Реализовано** — обход через ref struct, извлечение e/s/p/q/T |
| Интернирование символов | [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:165) | ✅ **Реализовано** — BTCUSDT/ETHUSDT/SOLUSDT без аллокаций |
| `ParseDecimalFromUtf8` Вариант **A** (ручной разбор байтов, zero-copy) | [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:208) | ❌ **НЕ реализован** — сейчас Вариант B: `stackalloc char[]` + копирование + `decimal.TryParse` |

**Вывод:** из плана `allocation-reduction-hotpath-plan.md` (шагов 1–3) остался только последний кусок — замена Варианта B на Вариант A в `ParseDecimalFromUtf8`.

---

## 2. Оставшаяся доработка

### 2.1 `ParseDecimalFromUtf8` — Вариант A (zero-copy, ниc TryParse)

**Текущая реализация (Вариант B, [`BinanceWebSocketClient.cs:208-219`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:208)):**
```csharp
Span<char> chars = stackalloc char[utf8.Length];
for (int i = 0; i < utf8.Length; i++)
    chars[i] = (char)utf8[i];
if (decimal.TryParse(chars, NumberStyles.Number, CultureInfo.InvariantCulture, out var result))
    return result;
return 0m;
```
**Недостатки:** копирование UTF-8→страницы, вызов `decimal.TryParse` (парсинг + internal), нет обработки знака/дробей вручную, `0m` как fallback маскирует ошибки.

**Целевая реализация (Вариант A):** ручное накопление целой и дробной частей по байтам без копирования в стек-буфер:
```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
private static decimal ParseDecimalFromUtf8(ReadOnlySpan<byte> utf8)
{
    if (utf8.IsEmpty) return 0m;
    bool neg = false;
    int i = 0;
    if (utf8[i] == (byte)'-') { neg = true; i++; }
    if (i >= utf8.Length) return 0m;

    long whole = 0;
    long frac = 0;
    int fracLen = 0;
    bool hasFrac = false;

    while (i < utf8.Length && utf8[i] != (byte)'.')
    {
        byte c = utf8[i];
        if (c < (byte)'0' || c > (byte)'9') return 0m; // некорректный символ
        whole = whole * 10 + (c - (byte)'0');
        i++;
    }
    if (i < utf8.Length) // есть '.'
    {
        hasFrac = true;
        i++; // skip '.'
        while (i < utf8.Length)
        {
            byte c = utf8[i];
            if (c < (byte)'0' || c > (byte)'9') return 0m;
            frac = frac * 10 + (c - (byte)'0');
            fracLen++;
            i++;
        }
    }
    long scale = 1;
    for (int k = 0; k < fracLen; k++) scale *= 10;
    decimal result = whole + (frac / (decimal)scale);  // целая + дробная
    return neg ? -result : (hasFrac ? result : whole);
}
```
> Учтите: `whole * (decimal)scale + frac` вместо `whole + frac/scale` — точнее (избегает двух операций и потенциальной потери). Замена при реализации на `dec = whole * scale + frac; dec /= scale;`.

**Требования:**
- Поддержка: целые (`"100"`), дробные с `.` (`"0.001"`, `"12345.678"`), отрицательные (`"-0.5"`).
- Не поддерживает экспоненты (`1e5`) — для Binance trade-цен не требуется; не схан находиться, чтобы не плодить неверные значения.
- Fallback 0m сохранить только для некорректного ввода (защита от кривого JSON), но структура парсинга лучше сообщает, чем `decimal.TryParse`.

### 2.2 Проверка краевых случаев (тесты)

Добавить/дополнить юнит-тесты `ParseDecimalFromUtf8`-эквивалент (через `ParseTradeMessage` или параметризованный тест):
- `0`, `100`, `-0.5`, `0.001`, `12345.678`, `123456789.123` (переполнение целой части за пределы `long` → уход в некорректный `0` или расширение до `decimal` накопления — решить).
- Гарантировать поведение при отсутствии `'.'` и при `"-"` без цифр.

---

## 3. Риски и ограничения

- **Переполнение `long` при целой части >9e18** — реальные цены крипты ≤ ~1e5, переполнение нереалистично, но тест на большие значения с явным `0m`-fallback допустим.
- **Числа агрессивны** (`123456789.123`): фракция из 9+ знаков — дезинтеграция; Binance ограничивает ≤8 — ок.
- **Точность:** накопление через `whole*scale + frac` избегает двойного деления и соответствует conventional rounding, эквивалентно `decimal` из целочисленной базы — проверить тестом эквивалентности с `decimal.TryParse`.

## 4. Критерий успеха

- Аллокации hot-path **не увеличиваются**; `ParseDecimalFromUtf8` не использует `stackalloc`/`TryParse`.
- Все существующие тесты (в т.ч. `WebSocketMessageReceiverTests`, разбор Binance message) проходят.
- Бенчмарк/повторный прогон: аллокации ≤ текущих (цель — не введи регрессий; выигрыш CPU в парсинге decimal, не в объёме аллокаций, т.к. текущий Вариант B уже без heap-аллокаций).