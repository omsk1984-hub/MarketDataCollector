# План снижения аллокаций на hot path (WebSocket → Channel)

## Контекст и цель

Прогон 20260731 показал: **2.0 GB аллокаций за 71 сек (~28 MB/сек)**, **8 Gen2 сборок**, **LOH-фрагментация 4.1 MB**. Это при нагрузке ~21K WS-сообщений/сек.

### Важный фактор деградации (учтено)
Деградация (дропы 13.3%, batch write до 7.8s) вызвана **внешним фактором** — все метрики, БД и процесс работали на **одном диске**. Это не устраняется кодом приложения (это инфраструктура). **Основная задача плана — не чинить диск, а снизить объём аллокаций на hot path**, чтобы снизить GC-давление и освободить CPU/IO.

### Целевой профиль аллокаций
Сейчас на каждый из 21K тиков/сек на входе аллоцируется как минимум:

| Источник | Кол-во аллокаций/тик | Файл |
|---|---|---|
| `Encoding.UTF8.GetString()` | 1 string | [`WebSocketMessageReceiver.cs:130`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:130) |
| `JsonDocument.Parse(message)` | ~N JsonElement | [`BinanceWebSocketClient.cs:74`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:74) |
| `decimal.Parse(p.GetString())` | 2 (string "p" + string "q") + parse | [`BinanceWebSocketClient.cs:81-84`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:81) |
| `s.GetString()` для ticker | 1 string (необходим) | [`BinanceWebSocketClient.cs:79`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:79) |

---

## Шаг 0. Анализ `allocation_trace_20260731_051705.nettrace` через SpeedScope

**Цель:** эмпирически подтвердить источники аллокаций ДО внесения изменений (baseline).

**Действия:**
1. Убедиться, что файл `traces/allocation_trace_20260731_051705.nettrace` существует (в текущем workspace папка `traces/` пуста — trace из прогона не сохранён; при необходимости **переснять** через `.\run_all_metrics.ps1`).
2. Конвертировать: `dotnet-trace convert --format speedscope <file>.nettrace --output <file>.speedscope.json` (уже автоматически делается в [`collect-trace.ps1`](scripts/collect-trace.ps1) → [`Convert-TraceToSpeedScope`](scripts/common-functions.ps1:295)).
3. Открыть `.speedscope.json` в [speedscope.app](https://www.speedscope.app), переключить **"Left Heavy"** — показать накопительные call stacks по времени/аллокациям.
4. Зафиксировать **Top-N allocators** по `gc-verbose` allocation stacks (ожидаемо: `System.Text.Json`, `decimal`, `Encoding.UTF8`, `MemoryStream`).

**Критерий выхода:** точный список функций, дающих ≥ 80% аллокаций, с процентом от общего объёма.

> ⚠️ Если trace недоступен — провести пробный прогон с `-Mode trace -TraceDuration 60` перед началом правок.

---

## Шаг 1. Рефакторинг `WebSocketMessageReceiver`: проброс `ReadOnlyMemory<byte>` вместо `string`

**Проблема:** [`WebSocketMessageReceiver.cs:130`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:130) делает `Encoding.UTF8.GetString(...)` — одна аллокация string на **каждое** сообщение (21K/сек). При этом `MemoryStream` уже держит байты — лишний копирующий декодинг.

**Целевая цепочка:** WS-байты → `ReadOnlyMemory<byte>` → `Utf8JsonReader` → структуры. Без промежуточной строки.

**Изменения:**
1. Сменить сигнатуру вызова в [`WebSocketMessageReceiver.cs:38-42,132-133`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:38):
   - `Func<string, Task> processMessage` → `Func<ReadOnlyMemory<byte>, Task> processMessage`
   - `Action<string>? onMessageReceived` → `Action<ReadOnlyMemory<byte>>? onMessageReceived`
2. В `RunLoopCoreAsync` вместо `Encoding.UTF8.GetString` передавать `messageStream.GetBuffer()` как `new ReadOnlyMemory<byte>(buffer, 0, (int)messageStream.Length)` — **без декодинга и без аллокации**.
3. `BaseWebSocketClient`:
   - [`ProcessMessageAsync(string)`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:273) → `ProcessMessageAsync(ReadOnlyMemory<byte>)` (виртуальный, override в наследниках).
   - [`OnMessageReceived(string)`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:395) → принимает байты; счётчики `WsMessagesReceived` не зависят от строки — менять только сигнатуру/вызов `MessageReceived`.
   - Событие `MessageReceived` (тип `EventHandler<string>`) — при необходимости оставить как есть, но в `OnMessageReceived` больше **не** декодировать строку для метрик; декодирование только если внешний подписчик реально требует string.

**Ожидаемый эффект:** −1 аллокация string на сообщение (21K/сек).

> ⚠️ Требует согласования интерфейса `IWebSocketMessageReceiver` и тестов (`WebSocketMessageReceiverTests`).

---

## Шаг 2. Парсинг JSON через `Utf8JsonReader` поверх `ReadOnlySpan<byte>`

**Проблема:** [`BinanceWebSocketClient.cs:74`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:74) использует `JsonDocument.Parse(string)` — строит DOM-дерево (`JsonElement` nodes) на каждый тик.

**Целевой подход:** ручной парсинг через `Utf8JsonReader` (ref struct, **ноль аллокаций**), извлекая только нужные поля.

```csharp
// Эскиз (внутри ProcessMessageAsync(ReadOnlyMemory<byte> message))
var json = message.Span;
var reader = new Utf8JsonReader(json);

// Сканируем токены, матчим "e","s","p","q","T" и читаем значения
if (reader.TokenType == JsonTokenType.StartObject) {
    while (reader.Read()) {
        if (reader.TokenType == JsonTokenType.PropertyName) {
            var prop = reader.ValueSpan; // ReadOnlySpan<byte>, без аллокации
            if (prop.SequenceEqual("p"u8)) {
                reader.Read();
                price = ParseDecimalFromUtf8(reader.ValueSpan); // см. Шаг 3
            }
            else if (prop.SequenceEqual("T"u8)) { /* GetInt64() */ }
            else if (prop.SequenceEqual("s"u8)) {
                reader.Read();
                ticker = reader.GetString(); // ЕДИНСТВЕННАЯ необходимая string
            }
            // ...
        }
    }
}
```

**Изменения в [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs):**
1. Переопределить `ProcessMessageAsync(ReadOnlyMemory<byte>)`.
2. Заменить `JsonDocument.Parse` + `TryGetProperty` на прямой обход `Utf8JsonReader`.
3. `ticker` из `reader.GetString()` — оставить (нужен для `TickData.Ticker`, ключа дедупликации, маршрутизации). Это неизбежная аллокация.
4. `timeMs` — `reader.GetInt64()` (поле `T` это JSON number).
5. Проверить: у Binance `"p":"0.001"` и `"q":"100"` — **JSON strings**, поэтому их разбор — отдельный шаг (Шаг 3).

**Ожидаемый эффект:** убирает `JsonElement`-node аллокации (~N/сообщение) и одну временную string на цену/объём.

---

## Шаг 3. Разбор decimal без аллокации строки

**Проблема:** [`BinanceWebSocketClient.cs:81-84`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:81) — `decimal.Parse(p.GetString())`: `GetString()` аллоцирует string, затем парсинг.

**Ограничение BCL:** в .NET 8 нет `decimal.Parse(ReadOnlySpan<byte>)` — только `decimal.Parse(ReadOnlySpan<char>)`. Поэтому:

**Вариант A (рекомендуемый, zero-alloc):** кастомный парсер UTF-8→decimal на `ReadOnlySpan<byte>`.
```csharp
private static decimal ParseDecimalFromUtf8(ReadOnlySpan<byte> s)
{
    // Ручной разбор: знак, целая часть, '.', дробная часть
    // Без decimal.Parse — работает прямо по байтам, ноль аллокаций
}
```
Нужно покрыть: знак, десятичная точка, максимум 8 знаков после запятой (Binance-формат), exponent (не требуется для типичных цен — покрыть тестами).

**Вариант B (проще, но с аллокацией на `stackalloc`):**
```csharp
Span<char> chars = stackalloc char[s.Length];
// Декодировать UTF-8 → UTF-16 в stackalloc
decimal.TryParse(chars, ...);
```
Аллокации в куче нет (stack), но есть CPU на декодинг. Менее предпочтителен.

**Критерий выбора:** Вариант A, если ручной парсер покрыт юнит-тестами (цены с отрицательными значениями, `0`, до 8 знаков). Иначе B как компромисс.

**Ожидаемый эффект:** −2 string аллокации на тик (цена + объём).

---

## Шаг 4. Оценка и внедрение `source-generated JsonSerializerContext`

**Контекст:** для **read-only hot path** (парсинг входящих тиков) source generator НЕ даст выигрыша в аллокациях против ручного `Utf8JsonReader` — он просто генерирует код с теми же `GetString`/`GetDecimal` вызовами. Его реальная польза — **десериализация в POCO** и для **Kafka-продюсера** (сериализация свечей).

**Решение по плану:**
1. **Входящие тики (Binance):** НЕ внедрять JsonSerializerContext — заменить на ручной `Utf8JsonReader` (Шаг 2). Это даёт максимум снижения аллокаций.
2. **Kafka-продюсер [`KafkaCandleProducer.cs`](src/MarketDataCollector.Infrastructure/Kafka/KafkaCandleProducer.cs):** внедрить `[JsonSerializable]`-контекст для сериализации свечей — убрать reflection-ветку `System.Text.Json`. Добавить в `.csproj` `<JsonSerializerIsReflectionEnabledByDefault>` при необходимости и `partial`-класс контекста.
3. Создать `MarketDataTelemetryJsonContext` / `MarketDataJsonContext` (в Infrastructure) с `[JsonSourceGenerationOptions(PropertyNamingPolicy = CamelCase)]`.

**Ожидаемый эффект:** снижение аллокаций на пути Kafka-сериализации (не является главным hot path, но убирает рефлексию).

---

## Шаг 5. Контрольный замер (до/после)

**Методика:** повтор прогона на **раздельных дисках** (изолировать внешний фактор деградации):
1. `.\run_all_metrics.ps1` с теми же параметрами (1.5M тиков).
2. Снять `counters_*.csv` → сравнить:
   - `gc_alloc_total` (должно снизиться с ~2 GB)
   - `gc_gen2_count` (8 → ниже)
   - `gc_committed` (301 MB → ниже)
   - дропы / эффективность записи
3. Снять `allocation_trace` → проверить, что Top allocators сместились (ticker string и внутренние `System.Text.Json` runtime-аллокации должны уйти).

**Критерий успеха:** снижение аллокаций на ≥ 30–50% на hot path; снижение Gen2; без регресса в throughput.

---

## Порядок реализации

| # | Задача | Файл | Риск |
|---|---|---|---|
| 0 | Анализ trace (baseline) | `traces/*.nettrace` | trace может отсутствовать |
| 1 | Проброс `ReadOnlyMemory<byte>` | `WebSocketMessageReceiver.cs`, `BaseWebSocketClient.cs`, интерфейс, тесты | ломает публичные сигнатуры/тесты |
| 2 | Ручной `Utf8JsonReader` | `BinanceWebSocketClient.cs` | изменения формата парсинга |
| 3 | `ParseDecimalFromUtf8` | `BinanceWebSocketClient.cs` + тесты | риск корректности чисел |
| 4 | JsonSerializerContext для Kafka | `KafkaCandleProducer.cs`, `.csproj` | низкий |
| 5 | Контрольный замер | `metrics.ps1` | — |

---

## Ключевые файлы
- [`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs) — проброс байтов
- [`BaseWebSocketClient.cs`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs) — виртуальный `ProcessMessageAsync`
- [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs) — ручной парсинг
- [`TickData.cs`](src/MarketDataCollector.Domain/Entities/TickData.cs) — модель (не меняется)
- [`WebSocketMessageReceiverTests.cs`](tests/MarketDataCollector.Tests/Core/Clients/WebSocketMessageReceiverTests.cs) — обновить под новые сигнатуры
