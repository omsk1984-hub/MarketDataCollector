# План: Выполнение рекомендаций из «diagnose-slow-batch-write-A0-result»

**Дата:** 13.09.2026
**Источник:** `plans/diagnose-slow-batch-write-A0-result.md` (§ «Рекомендации (обновляют основной план устранения)»)
**Приоритет рекомендаций:** P0 (схема/партиционирование) → P1 (клиентская конкурентность/batchSize) → P2 (логирование хвостов).

---

## Корневая причина (кратко, из A0)

Серверный INSERT быстр (35–48 мс на 2500). Медленные батчи — **хвосты на пике нагрузки**, потому что:

1. **Все 1.65M строк уходят в `rawticks_default`** вместо day-партиций: FakeTickServer генерирует `timestamp` по эпохе 1970 (`syntheticTimestamp = tradeId`, `tradeId` стартует от ~1e8).
2. Вся нагрузка и уникальность-индекс (`IX_rawticks_ticker_exchange_timestamp`) бьют в одну растущую таблицу.
3. Конкурентная запись нескольких батчей + клиентская накладная Npgsql.

---

## Что я обнаружил в коде (подтверждение)

- [`TickGeneratorService.GenerateTick()`](tests/FakeTickServer/TickGeneratorService.cs:427): `var syntheticTimestamp = tradeId;` затем пишется в JSON-поля `E` и `T`.
- [`BinanceWebSocketClient.cs`](src/MarketDataCollector.Infrastructure/Clients/BinanceWebSocketClient.cs:91): воркер читает поле `T` как `timeMs` → `DateTimeOffset.FromUnixTimeMilliseconds(T)`. `FromUnixTimeMilliseconds(~1e8)` ≈ **1970-01-11**.
- [`appsettings.LoadTest.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.LoadTest.json:19): `MinBatchSize = MaxBatchSize = 2500` → адаптивный batch size выключен (между min/max нет диапasона).
- Логирование долгих батчей отсутствует: `_writeDurationWarningMs` (см. [`MarketDataProcessor.cs`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:642)) используется только для адаптивного снижения batchSize, а `LogBatchSaved` — Debug и без длительности.

---

## Изменения

### 1. [P0] FakeTickServer: реальный `timestamp` вместо эпохи (tradeId)

**Файл:** `tests/FakeTickServer/TickGeneratorService.cs`

- В `GenerateTick` генерировать `syntheticTimestamp` как **миллисекунды эпохи, близкие к `now()`**, вместо `= tradeId`.
- Сохранить `_globalTradeId` только для уникальности (поле `t` в JSON) — не путать с временем.
- Требование уникальности `(ticker, exchange, timestamp)`: при реальном времени несколько тиков в одну и ту же миллисекунду дадут колизию ключа. **Решение:** оставить timestamp реальным, но развязать его от tradeId; при DupPercent=3 дубли всё равно существуют. Однако чтобы не плодить конфликты в уникальном индексе, используем timestamp на основе `now()` + **синтетический монотонный сдвиг внутри мс** (т.е. `nowMs * 1000 + microOffset`), сохраняя реальную дату, но делая ключ почти всегда уникальным. Подробности — ниже в коде.

**Способ (минимально инвазивный):**
- `timestampMs = DateTime.UtcNow.EpochMilliseconds` — реальное текущее время.
- Для сохранения монотонности и уникальности в пределах одной мс: `syntheticTimestamp = timestampMs * 1000 + Interlocked.Increment(ref _microCounter) % 1000`, но поскольку `numeric`/`bigint`-временной диапазон воркера = `FromUnixTimeMilliseconds(значение / 1000)`, деление на 1000 даст исходную дату. **Осторожно:** воркер берёт `T` как `GetInt64()` и делает `FromUnixTimeMilliseconds`. Если надо сохранить реальную дату до мс — нужно, чтобы `T` имел порядок **миллисекунд**, а не микросекунд.

**Простое и корректное решение (принято):**
- `syntheticTimestamp = nowMs` (мс эпохи, реальное время).
- Уникальность ключа `(ticker, exchange, timestamp)` при DupPercent>0 и колизии мс — допустима (это аналогично реальной бирже, где могут быть дубли), НО текущий dedup-кэш воркера (`DeduplicationCache`) держит только ~1.2с данных и отсеивает дубли по `(ticker, exchange, timestamp)`. Если два тика попадут в одну мс — второй будет отфильтрован кэшем/БД как дубль. **Риск: потеря части тиков на высоком RPS (25000/с → 25 тиков/мс при 3 символах).**

> ⚠️ Это принципиальная проблема: при 25 000 тиков/с подряд тики в одну мс неизбежны. Реальная Binance отдаёт `T` в **миллисекундах** и допускает дубли ключей на практике редко (низкий RPS на символ). Для нагрузочного теста нужен **монотонный timestamp**, близкий к now.

**Финальное решение (сохраняет реальную дату И уникальность):**
- `syntheticTimestamp = nowMs * 1_000_000 + (внутримс-счётчик)` — НО воркер делает `FromUnixTimeMilliseconds(syntheticTimestamp)`.
  Такое значение уже не миллисекунды (это нано/микро-масштаб) → дата снова уедет.

Учитывая контракт воркера (`FromUnixTimeMilliseconds(T)`), **единственный корректный способ** сохранить и реальную дату, и уникальность — сделать `T` **монотонным числом миллисекунд, стартующим от текущей даты**:

- В конструкторе: `_syntheticTimeMs = DateTime.UtcNow.EpochMilliseconds;`
- В `GenerateTick`: `syntheticTimestamp = Interlocked.Increment(ref _syntheticTimeMs);` — монотонный, всегда ≥ now, уникален, `FromUnixTimeMilliseconds` даёт дату близкую к текущей.

Это решает всё:
- Данные лягут в day-партиции по `timestamp` (SQL native partition по `timestamp`).
- Ключ `(ticker, exchange, timestamp)` уникален (монотонный мс), т.к. `timestamp` различается хотя бы на 1 мс между соседними тиками.
- `rawticks_default` перестанет расти.

Поле `E` (event time для трейда) — поставим то же значение, как и раньше (Binance пишет одинаковые `E` и `T`).

### 2. [P1] Разблокировать адаптивный batchSize (2500 → 5000/10000)

**Файл:** `src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.LoadTest.json`

- `MinBatchSize`: `2500` → `2500` (оставить, нижний порог).
- `MaxBatchSize`: `2500` → `10000` (сервер показывает ~48 мс на 2500 → 10k даст тот же round-trip оверхед при ×4 пропускной).
- `BatchChannelCapacity`: `40` → оставить (40 × 10k = 400k тиков буфер — ок для LoadTest с ChannelCapacity=150k input).

> ⚠️ Пересчитать: `ChannelCapacity=150000` входной, `MaxBatchSize=10000`, `BatchChannelCapacity=40` → буфер батчей 400k > входного 150k. Это нормально (батчи формируются из входного канала, max буфер 400k тиков ожидают записи). Но стоит поднять `ChannelCapacity` при желании большей загрузки. Для A0-перепрогона оставляем 150k.

### 3. [P2] Логирование хвостов (долгих батчей)

**Файлы:**
- `src/MarketDataCollector.Application/Services/MarketDataProcessor.Logging.cs`
- `src/MarketDataCollector.Application/Services/MarketDataProcessor.cs`

Добавить Warning-лог, когда запись батча заняла больше `_writeDurationWarningMs` (сейчас `200.0` мс по умолчанию в LoadTest). Это даст видимость хвостов >200 мс, которые сейчас «слепят операцию».

- В `MarketDataProcessor.Logging.cs`: новый `[LoggerMessage(...)]` `LogSlowBatchWrite(long durationMs, int inserted, int batchSize, int channel)` с `Level = Warning`.
- В `MarketDataProcessor.ProcessBatchAsync` после `sw.Stop()`: если `_writeDurationWarningMs > 0 && sw.Elapsed.TotalMilliseconds > _writeDurationWarningMs` → `LogSlowBatchWrite(...)`.

---

## Верификация

1. **Сборка:** `cd tests/FakeTickServer && dotnet build` — кода генератора.
2. **Unit-тесты воркера** (затрагивается только MarketDataProcessor): `dotnet test tests/MarketDataCollector.Tests` — убедиться, что добавленный лог не ломает существующие тесты.
3. **Перепрогон LoadTest:**
   - Запустить `start_loadtest.ps1` (MaxTicks ~1.7M, Rps 25000).
   - После прогона проверить SQL:
     - `SELECT count(*) FROM rawticks WHERE timestamp > now() - interval '1 day';` — ненулевой (данные в актуальных партициях).
     - `SELECT count(*) FROM rawticks_default WHERE ...` — прирост ≈ 0 (или малый, только если тест стартовал на границе времени).
   - Сверить `ticks_batch_write_duration_milliseconds` — хвосты должны снизиться/исчезнуть.
4. **Проверить лог Worker** на `LogSlowBatchWrite` — фиксируются только батчи >200 мс (ожидаемо мало после P0+P1).

---

## Откат

- Изменения изолированы: 
  - P0 — только `tests/FakeTickServer/TickGeneratorService.cs` (тестовый генератор, не влияет на прод).
  - P1 — только `appsettings.LoadTest.json` (профиль LoadTest).
  - P2 — добавочный Warning-лог в hot path (влияет на прод минимально, но при желании закомментировать).
- Никаких изменений схемы БД/продакшн-кода, кроме лога, не вносим.