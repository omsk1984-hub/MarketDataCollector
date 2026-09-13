# План оптимизации по итогам анализа 2026-09-13 03:12

Исходный отчёт: [`plans/counters-analysis_20260913_031212.md`](counters-analysis_20260913_031212.md)

## Цель

Снизить три приоритетные проблемы по итогам профилирования:

| Приоритет | Проблема | Ключевой факт |
|---|---|---|
| **P1** | Медленная запись батчей в БД | avg 949 мс, p50 662 мс, max 3.13 с при пороге 200 мс; **4 прогона подряд идентично** (1 014 батчей, 949/3131). Очередь батчей ~37/40 — на грани переполнения. |
| **P2** | Аллокации `Decimal` | `.ctor(Decimal, Span<int16>)` 10.85% exclusive (топ-5 CPU); коррелирует с LOH-кучей 39.8 MB (вдвое выше базовых ~17 MB). |
| **P3** | Приём WebSocket — доминирующий CPU | `ReceiveAsync` 16.5% + `ManagedWebSocket.ReceiveAsync` 12.1% + `GetReceiveResult` 11.1% + `Task.FromResult` 14.1% ≈ **40% CPU**. |

## Кодовая база (контекст)

- **P1:** [`RawTickRepository.BulkInsertFastAsync(TickData)`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:476) → `INSERT ... SELECT unnest(...)` + `ON CONFLICT ... DO NOTHING`. Вызывается из [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:830) на каждый батч.
- **P2:** [`ParseDecimalFromUtf8`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:212) — построение `decimal` через `whole*10m + digit`, `frac/scale`, `whole+frac/scale`; на каждый tick 2 вызова (price, volume). `Decimal`-конструктор `.ctor(Decimal, Span<int16>)` — это внутренняя работа `decimal`-типа.
- **P3:** [`WebSocketConnectionManager.ReceiveAsync`](src/MarketDataCollector.Core/Clients/WebSocketConnectionManager.cs:107) → `ClientWebSocket.ReceiveAsync` → `GetReceiveResult`/`Task.FromResult` в `RunLoopCoreAsync` ([`WebSocketMessageReceiver.cs`](src/MarketDataCollector.Core/Clients/WebSocketMessageReceiver.cs:98)).

---

## P1. Медленная запись батчей

### Диагностика (что делает батч медленным)

1. **Снять тайминг фаз в `ProcessBatchAsync`:** обернуть отдельными Stopwatch: (а) `BulkInsertFastAsync` сетевое время, (б) время до первого `Receive` из Npgsql. Добавить лог-предупреждение с разбивкой при `writeDurationMs > WriteDurationWarningMs`.
2. **Проверить Npgsql-пакет:** `DecimalHelper.TruncateForDatabase` вызывается для каждой `price`/`volume` (см. `RawTick.cs:33-34`). Убедиться, что `numeric[]` + `unnest` не сериализуются в бОльшую текстовую форму (Npgsql кодирует `decimal[]` в бинарном виде).
3. **Конкурентная запись:** `Inserted` ≈ 2423/батч при 2500 — в коде `BatchChannelCapacity=40` (по классическому режиму) → до 20 параллельных INSERT в БД. Убедиться, что PostgreSQL при этом не упирается в row-locks/sync commit.

### Реализация (после подтверждения причины)

- **A. Профиль-вызов `BulkInsertFastAsync` по фазам** (секция «Диагностика» выше) — без code-изменений бизнес-логики, только диагностическая телеметрия.
- **B. `autocommit` / синхронный commit:** если подтверждён `fsync`-замедлитель — добавить конфиг-режим `BulkInsertFlushMode` (`sync` / `async`), управляющий `synchronous_commit=off` для батчевого консьюмера (через `SET synchronous_commit=off` в сессии Npgsql). **Осторожно:** снижает durability, применимо только в LoadTest.
- **C. Пул писателей vs один writer:** если подтверждена конкуренция INSERT — вернуть односекциональный режим записи (последовательная запись одним writer'ом), чтобы не упереться в contention в БД. Оценить экспериментально.

> **Решение о конкретном коде принимается ТОЛЬКО после п. Диагностика.** Не гадать: снять тайминги, затем выбрать A/B/C.

---

## P2. Аллокации `Decimal`

### Источник

`ParseDecimalFromUtf8` (BinanceWebSocketClient.cs:212-261) формирует `decimal` как:
```
whole = whole*10m + digit ...   // на каждую цифру целой части
frac  = frac*10m  + digit ...   // на каждую цифру дробной части
result = whole + (frac / scale) // финальная сборка
```

Каждый из этих `*10m`, `+`, `/` порождает работу `decimal`-типа (в т.ч. `.ctor(Decimal, Span<int16>)` из topN). Это **основной** источник P2 — на каждый tick 2 вызова (price + volume).

### Оптимизация (безопасное снижение числа операций с `decimal`)

1. **Скалярный накопитель вместо поразрядного умножения** внутри функции: предварительно распарсить цифры в `long` (целая часть) и `long` (дробная часть, с подсчётом scale), затем **один** `decimal`-конструктор в конце. Это заменяет `O(n)` операций `decimal` (где n — число цифр, до ~10+8) на **одну** операцию.

   Точность: Binance price/quantity — до 8 знаков после точки. `long` безопасен для целой части (≤ 10^10) и дробной части (≤ 10^8). Итоговое значение `whole + frac/10^scale` собирается за одну арифметическую операцию `decimal` (2 оператора: `/` и `+`), а не за N.

2. **Опциональный fast-path для «короткого» числа:** если целая часть < 2^31 и scale==0 → `whole` как есть (без дробной части).

3. **Гарантия формата (18,8):** после сборки — `DecimalHelper.TruncateForDatabase` (уже есть в `RawTick.cs`), не менять семантику.

### Проверка корректности

- Юнит-тесты на `ParseDecimalFromUtf8`: типовые Binance-строки `"1000.50"`, `"0.001"`, `"100"` (scale 0), `"12345.678"`, отрицательные `"-123.45"`, edge `"0.00000001"` (8 знаков), переполнение целой части (→ clamp/Truncate).

---

## P3. Приём WebSocket (доминирующий CPU)

### Наблюдения из topN

- `WebSocketConnectionManager.ReceiveAsync` 16.5% + `ManagedWebSocket.ReceiveAsync` 12.1% + `GetReceiveResult` 11.1% — сам приём/разбор кадра.
- `Task.FromResult` 14.1% — значимый, но это учёт завершённых read-операций (не источник аллокаций сам по себе).
- `Byte[].Rent` 6.98% — переиспользование буферов уже применяется.

### Оптимизация

> **`ManagedWebSocket.GetReceiveResult`/`ReceiveAsync` — это реализация JDK/стандартной библиотеки (`System.Net.WebSockets`), её код нам недоступен.** Менять его нельзя. Поэтому фокус — на стороне вызова (наш слой).

1. **Сократить расходы на вызов ReceiveAsync в `RunLoopCoreAsync`:**
   - Кэшировать `_connectionManager.IsConnected` внутри итерации без доп. проверки состояния на каждом кадре (в цикле уже есть `while (!cancellationToken...)`).
   - Использовать **один** `ArraySegment<byte>(tempBuffer)` за весь цикл (сейчас создаётся на каждую итерацию — микроаллокация).
   
2. **`Task.FromResult`:** проверить, что `ProcessMessageAsync` в `BinanceWebSocketClient` возвращает `Task.CompletedTask` **без** создания нового `Task` (уже так, см. стр. 106). Подтвердить, что `GetReceiveResult` не вызывает `FromResult` на каждый кадр на нашей стороне.

3. **Пропуск `ReceiveAsync`-обёртки:** `WebSocketConnectionManager.ReceiveAsync` — тривиальная обёртка над `ws.ReceiveAsync`. Снизить стоимость можно передачей `IClientWebSocket` напрямую в `WebSocketMessageReceiver` (убрать один уровень indirection) — но **только если это не ухудшит тестируемость** (мок IClientWebSocket уже есть). Оценить после диагностики.

4. **Buffered batch-чтение в один вызов** `ReadAtLeastAsync`/`ReadAtLeast` (Standard WebSockets `ClientWebSocket` содержит встроенный буфер) — если `ClientWebSocket.ReceiveAsync` уже буферизует фреймы на своей стороне, дополнительная агрегация не нужна.

> Реализация P3 ограничена: горячие методы принадлежат стандартной библиотеке. Реальный выигрыш — устранение накладных расходов нашего слоя вызова (скр. 1–2), а не самого чтения.

---

## Порядок выполнения

1. **Диагностика P1** (тайминги фаз запроса) — решает, какой вариант A/B/C внедрять.
2. **P2** (скалярный парсинг Decimal) — независим, даёт и CPU, и LOH-снижение.
3. **P3** (наш слой ReceiveAsync/ArraySegment/обёртка) — независим.
4. Прогон loadtest (с согласия пользователя — правила `run_all_profiler.ps1`/`FakeTickServer`) + `metrics-analysis`.
5. Сравнить с базовым `counters-analysis_20260913_031212.md`.

## Критерии успеха

| Метрика | Базово | Цель |
|---|---|---|
| Batch duration max/avg | 3 131 / 949 мс | max < 1500 мс, avg < 500 мс |
| `Decimal`-конструктор в topN | 10.85% exclusive | < 5% |
| LOH-куча (финал) | 39.8 MB | < 25 MB |
| ReceiveAsync-доля топ-N | ~40% суммарно | заметно ниже |
| Дропы | 0 | 0 (не регрессировать) |

## Открытые вопросы (вероятно потребуют уточнения у пользователя)

- Можно ли менять `synchronous_commit`/`autocommit` в LoadTest (durability vs скорость)?
- Допустимо ли менять `BatchChannelCapacity`/число параллельных writer'ов для проверки P1-гипотезы о конкуренции INSERT?