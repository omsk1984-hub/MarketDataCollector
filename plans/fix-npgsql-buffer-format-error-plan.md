# План исправления: "Buffer requirements for format not respected" в BulkCopyAsync

## Описание проблемы

При выполнении [`BulkCopyAsync`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:235) периодически возникает ошибка:

```
System.InvalidOperationException: Buffer requirements for format not respected, expected no IO to be required.
```

Ошибка происходит в [`Npgsql`](src/MarketDataCollector.Infrastructure/MarketDataCollector.Infrastructure.csproj:14) (через EF Core `ExecuteSqlRawAsync`) при передаче массивов `decimal[]` для параметров `@prices` и `@volumes` с типом `NpgsqlDbType.Numeric`.

## Root Cause

В Npgsql 8.0.0 (который используется в проекте) тип `Numeric` (`decimal`) **не поддерживает buffered mode** — для его сериализации в бинарный формат PostgreSQL требуется асинхронная операция (IO). Однако `PgArrayConverter` при записи массива ожидает, что каждый элемент будет записан через `PgBufferedConverter` без IO.

Это известный баг Npgsql 8.0.0:

- `NpgsqlDbType.Numeric` в бинарном формате массива требует IO, но конвертер помечен как buffered
- `PgBufferedConverter` вызывает `ThrowIORequired()`, когда нижележащий конвертер пытается выполнить IO
- Ошибка проявляется **интермиттентно**, т.к. зависит от размера буфера и состояния соединения

**Пострадавшие параметры:** `@prices` (`decimal[]`) и `@volumes` (`decimal[]`) — строки 275-276 в [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs:271).

## Воспроизведение

Ошибка возникает в `MarketDataProcessor` при вызове [`ProcessBatchAsync`](src/MarketDataCollector.Application/Services/MarketDataProcessor.cs:591):

```
MarketDataProcessor.ProcessBatchAsync → RawTickRepository.BulkCopyAsync → ExecuteSqlRawAsync
```

Конфигурация с `BatchSize=2000` и `ConsumerCount=3` (`UseSingleConsumer=false`) — см. [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:22-29).

## Решение

### Вариант A (рекомендуемый): Использовать `string[]` для decimal-массивов с кастом в SQL

**Суть:** Заменить `decimal[]` / `NpgsqlDbType.Numeric` на `string[]` / `NpgsqlDbType.Text` с приведением типов в SQL через `::text[]` → `::numeric`.

Это полностью обходит баг Npgsql, т.к. `Text` — это примитивный тип, который гарантированно поддерживает buffered mode в массивах.

#### Изменения в [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs)

**1. Типы массивов** (строки 245-246):

```csharp
// Было:
var prices = new decimal[count];
var volumes = new decimal[count];

// Стало:
var prices = new string[count];
var volumes = new string[count];
```

**2. Заполнение массивов** (строки 257-258):

```csharp
// Было:
prices[i] = e.Price;
volumes[i] = e.Volume;

// Стало:
prices[i] = e.Price.ToString(CultureInfo.InvariantCulture);
volumes[i] = e.Volume.ToString(CultureInfo.InvariantCulture);
```

**3. Параметры Npgsql** (строки 275-276):

```csharp
// Было:
new("@prices", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Numeric) { Value = prices },
new("@volumes", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Numeric) { Value = volumes },

// Стало:
new("@prices", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = prices },
new("@volumes", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value = volumes },
```

**4. SQL-запрос** (строки 265-269):

```sql
-- Было:
SELECT unnest(@ids), unnest(@tickers), unnest(@prices), unnest(@volumes),
       unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)

-- Стало:
SELECT unnest(@ids), unnest(@tickers), unnest(@prices::text[])::numeric, unnest(@volumes::text[])::numeric,
       unnest(@timestamps), unnest(@exchanges), unnest(@receivedats), unnest(@normalizeds)
```

**5. Добавить using:**

```csharp
using System.Globalization;
```

### Вариант B (дополнительно): Обновить Npgsql до последней patch-версии 8.x

Обновить `Npgsql.EntityFrameworkCore.PostgreSQL` с 8.0.0 до 8.0.11+ (последняя 8.x).

```xml
<PackageReference Include="Npgsql.EntityFrameworkCore.PostgreSQL" Version="8.0.11" />
```

Это может исправить баг на уровне библиотеки, но не гарантировано — поведение Npgsql для `Numeric` в массивах может не измениться в patch-релизах. **Основной fix — Вариант A.**

### Вариант C (альтернативный): Уменьшить размер батча

Уменьшить `BatchSize` с 2000 до ~800 в [`appsettings.json`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/appsettings.json:23). При меньших массивах вероятность достижения лимита буфера Npgsql снижается.

**Не рекомендуется как sole fix** — только маскирует проблему, не устраняя root cause.

## Критерии успеха

1. Ошибка `Buffer requirements for format not respected` не появляется в логах при длительной работе (>1M тиков)
2. Производительность вставки не снижается (те же ~17-18K ticks/sec)
3. Все тесты проходят: `dotnet test`

## Порядок выполнения

1. Внести изменения в [`RawTickRepository.cs`](src/MarketDataCollector.Infrastructure/Repositories/RawTickRepository.cs) (Вариант A)
2. Опционально: обновить пакет Npgsql (Вариант B)
3. Запустить интеграционные тесты
4. Запустить приложение с `run.ps1` и проверить отсутствие ошибок в логах

## Mermaid-диаграмма: поток данных после исправления

```mermaid
flowchart TD
    A[ProcessBatchAsync] --> B[RawTickRepository.BulkCopyAsync]
    B --> C[Формирование массивов]
    C --> D[prices: string[] - ToString InvariantCulture]
    C --> E[volumes: string[] - ToString InvariantCulture]
    C --> F[Остальные массивы без изменений]
    D --> G[NpgsqlParameter DbType=Text]
    E --> G
    F --> H[NpgsqlParameter соответствующих типов]
    G --> I[ExecuteSqlRawAsync]
    H --> I
    I --> J[SQL: unnest@... ::text[]::numeric]
    J --> K[PostgreSQL парсинг decimal]
    K --> L[INSERT ON CONFLICT DO NOTHING]
```
