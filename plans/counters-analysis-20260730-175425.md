# Анализ файла counters_20260730_175425.csv

**Дата сбора:** 2026-07-30 17:54:25 — 17:55:54 (~1.5 минуты)  
**Файл:** `counters/counters_20260730_175425.csv` (86,406 строк)

---

## 1. Общая статистика обработки тиков

| Метрика | Начало (17:54:25) | Середина (17:54:32) | Конец (17:55:54) |
|---|---|---|---|
| `ticks_incoming_count_total` | 41,768 | 235,075 | **1,500,000** |
| `ticks_processed_count_total` | 15,501 | 133,851 | **1,454,959** |
| `ticks_received_count_total` | 16,000 | 138,077 | **1,500,000** |
| `ticks_dropped_silently_count_total` | — | — | **55,000** |

### Ключевые выводы:
- Все 1.5M тиков поступили в канал (`received = incoming`), **потерь на входе нет**
- **55,000 тиков (3.67%) отброшены** механизмом `DropOldest` из-за переполнения bounded channel
- Процент записи: `1,454,959 / 1,500,000 = 97.0%` от общего числа тиков
- Пропускная способность: **~16,700 тиков/сек** на входе, **~16,200 тиков/сек** записано

---

## 2. Анализ канала и бэклога

| Время | `channel_fill_count` count | `channel_fill_count` sum | Средняя глубина канала |
|---|---|---|---|
| 17:54:25 | 1 | 0 | 0 |
| 17:54:32 | 2 | 49,108 | ~24,554 |
| 17:54:40 | 3 | 52,792 | ~17,597 |

**Пиковый бэклог:** `processor_channel_backlog_count = 93,912` (17:54:40)

**Динамика:**
- Канал пуст в начале
- Быстрое заполнение из-за того, что продюсер (WS) быстрее консьюмера (DB write)
- К концу прогона бэклог рассасывается — все 1.5M тиков прочитаны из канала

---

## 3. Статистика пакетной записи (batch writes)

### Размер батчей

| Параметр | Значение |
|---|---|
| Всего батчей (`batch_size_count`) | 161 |
| Всего записано тиков (`batch_size_sum`) | 344,333 |
| **Средний размер батча** | **~2,138** тиков |
| Адаптивный размер (средний) | ~2,100 тиков (sum=380,207 / count=181) |

**Распределение размеров:** большая часть батчей 2,500 тиков, начальные — 1,000 тиков, финальные «добивающие» батчи — от 309 до 901 тиков.

### Длительность записи (batch_size=2500)

| Метрика | Значение |
|---|---|
| Минимальная | ~33 ms |
| Максимальная | ~880 ms |
| Типичный диапазон | **75–250 ms** |
| Медианная оценка | ~100–150 ms |

### Эффективность вставки

`inserted_count / batch_size` = **96-97%** в среднем. Это означает ~3-4% дубликатов, отсеиваемых на уровне БД (ON CONFLICT DO NOTHING / уникальный ключ).

---

## 4. WebSocket-подключения

| Параметр | Начало (17:54:25) | Конец (17:55:54) |
|---|---|---|
| Активные соединения | 2 (BTC, ETH) | **3** (BTC, ETH, **SOL**) |
| `ws_messages_received` — btcusdt | 29,089 | **526,230** |
| `ws_messages_received` — ethusdt | 12,680 | **496,784** |
| `ws_messages_received` — solusdt | — | **476,986** |

**Наблюдение:** SOLUSDT подключён позже остальных. BTCUSDT — самый загруженный стрим (37% всех сообщений). Символы распределены равномерно.

---

## 5. .NET Runtime метрики (финальные)

| Метрика | Значение | Интерпретация |
|---|---|---|
| Assemblies loaded | 136 | Нормально |
| GC Allocations total | ~2.87 GB | Высокая аллокационная нагрузка |
| GC Gen0/Gen1/Gen2 collections | 10 / 4 / 5 | **5 Gen2 сборок** за 90 сек — много |
| GC Committed Memory | ~250 MB | Растёт (было 202MB) |
| Fragmentation Gen2 | ~2.4 MB | Признак фрагментации кучи |
| Fragmentation LOH | ~1.1 MB | Фрагментация large object heap |
| JIT Methods Compiled | 20,139 | Много JIT-компиляции (возможны горячие пути) |
| ThreadPool Completed Items | 76,514 | Активная работа ThreadPool |
| Lock Contention | 62 раза | Есть contended lock'и |
| Exceptions | 3 | Практически нет исключений |

---

## 6. Анализ аллокаций — нужен ли gcdump?

### 6.1. Что было в коде (до исправлений)

**Главный подозреваемый №1 — `BinanceWebSocketClient.cs:74`:**
```csharp
// БЫЛО:
var json = JObject.Parse(message);  // Newtonsoft.Json — полное DOM-дерево на каждое сообщение

// СТАЛО:
using var doc = JsonDocument.Parse(message);  // System.Text.Json — ref struct, минимальные аллокации
```
`JObject.Parse()` создавал полное DOM-дерево (`JObject` + `JProperty` + `JToken` + строки) на **каждое** WebSocket-сообщение. При 1.5M тиков за 90 секунд (~16,700/сек) — это **огромная GC pressure**.

**Исправлено:** Заменён на `JsonDocument.Parse()` (`System.Text.Json`) — `ref struct`, аллоцирует на порядок меньше.
✅ Тесты: 11 passed. Файл: [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs)

**Главный подозреваемый №2 — `TickAggregator.cs:64`:**
```csharp
// БЫЛО:
private class InMemoryCandle  // reference type!
{
    public string Ticker = null!;  // строки в куче — дублировали ключ словаря
    ...
}

// СТАЛО:
private struct InMemoryCandle  // value type — без строк, поля берутся из ключа
{
    public DateTime StartTime;
    ...
}
```

**Исправлено:** `InMemoryCandle` переделан из `class` в `struct`. Убраны поля `Ticker`, `Exchange`, `Interval` (теперь берутся из `AggregatorKey` словаря).
✅ Тесты: 23 passed. Файл: [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs)

### 6.2. Что даёт `dotnet-gcdump`?

| Что увидит | Полезно |
|---|---|
| Фрагментацию Gen2 / LOH | ✅ Подтвердит эффект исправлений |
| Остаточные аллокации в горячих путях | ⚠️ Только при снапшоте во время пика |
| **Где создаются объекты (stack trace)** | ❌ **Нет!** |

### 6.3. Ограничения gcdump

1. **Только snapshot** — видно, что есть на куче прямо сейчас. После GC короткоживущие объекты уже не видны.
2. **Сам gcdump триггерит Gen2 GC** — результат может быть искажён.
3. **Не показывает allocation call stacks** — не узнаете, какой `new` на hot path.

### 6.4. Что ещё нужно → `dotnet-trace` с allocation tracking

| Возможность | `gcdump` | `dotnet-trace` allocation | `dotnet-counters` |
|---|---|---|---|
| Total allocations | ✅ | ✅ | ✅ (уже есть) |
| **Allocation call stacks (откуда)** | ❌ | ✅ | ❌ |
| Gen2/LOH фрагментация | ✅ | ❌ | ❌ |
| Live instances | ✅ | ❌ | ❌ |
| Overhead | Высокий | Средний | Низкий |

---

## 7. Выполненные оптимизации

| Оптимизация | Файл | Статус | Тесты |
|---|---|---|---|
| `JObject.Parse` → `JsonDocument.Parse` | [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs) | ✅ | 11/11 passed |
| `class InMemoryCandle` → `struct InMemoryCandle` | [`TickAggregator.cs`](src/MarketDataCollector.Application/Services/TickAggregator.cs) | ✅ | 23/23 passed |

---

## 8. Ключевые проблемы и рекомендации

### Проблема 1: Drop тиков из-за переполнения канала (3.67%)
- **Причина:** Продюсер (WebSocket -> канал) быстрее консьюмера (канал -> батч -> БД)
- **Рекомендация:** Увеличить ёмкость bounded channel или перейти на unbounded channel с backpressure, либо увеличить размер батча раньше (адаптивный механизм срабатывает с задержкой)

### Проблема 2: Высокая GC-нагрузка
- **Причина:** 5 Gen2 сборок за 90 секунд + 2.87 GB аллокаций
- **Рекомендация:** Исследовать горячие аллокации (возможно, парсинг JSON, строки), использовать пулы массивов (`ArrayPool`), Struct-based парсинг

### Проблема 3: Высокая вариативность времени записи
- **Причина:** batch_write_duration_ms варьируется от 33ms до 880ms
- **Рекомендация:** Проверить PostgreSQL на медленные запросы, возможно влияние других процессов или checkpoint'ов

### Проблема 4: Бэклог в канале ~94K элементов
- Указывает на дисбаланс между скоростью чтения из WebSocket и записи в БД
- Рекомендуется мониторить backlog как ключевой health-check метрики
