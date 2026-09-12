# План: Вывод статистики дублей/уникальных тиков в loadtest-скрипт

## 1. Текущее состояние

Сейчас в конце [`start_loadtest.ps1`](start_loadtest.ps1) выводится только:

```
  FakeTickServer завершён: 1700000 тиков, isLimitReached=true.
```

Из логов можно извлечь следующие данные:

### FakeTickServer

| Метрика | Значение | Откуда |
|---------|----------|--------|
| Всего сгенерировано | 1 700 000 | health-ответ сервера (`maxTicks`) |
| Уникальных | 1 648 817 | последняя строка `fake_server_out.log` (строка 78: `Уникальных: 1648817`) |
| Дублей | 51 183 (3.0%) | `уникальных=1648817, дублей=48929` (сумма `1700000 - 1648817 = 51183`) |

### Worker (БД)

| Метрика | Значение | Откуда |
|---------|----------|--------|
| Вставлено в БД | 1 413 687 | итоговая строка worker_out.log (строка 139: `вставлено: 1413687`) |
| Получено из канала | 1 457 500 | та же строка (`получено из канала: 1457500`) |
| Дропнуто (DropOldest) | 242 500 | `backlog (incoming-received): 242500` |

### Расхождение уникальности

```
FakeServer уникальных:          1 648 817
Дропнуто каналом:                 242 500
Из них уникальных (~14.3%):   ~ 235 225
Доехало уникальных до БД:     ~ 1 413 592
Вставлено в БД:                   1 413 687
Отсеяно ON CONFLICT DO NOTHING:  ~95
```

## 2. Цель

1. **Вывести** количество уникальных тиков от FakeTickServer в консоль loadtest-скрипта
2. **Вывести** количество записанных в БД (уникальных после дедупликации)
3. **Сравнить** две цифры для визуального контроля потерь

## 3. Варианты реализации

### A. Парсинг логов (быстро, минимальные изменения)

**Где меняем**: [`start_loadtest.ps1`](start_loadtest.ps1)

- После ожидания FakeTickServer (шаг 6) парсим последнюю строку `fake_server_out.log` с `Уникальных:`
- После graceful остановки Worker (шаг 7) парсим итоговую строку из `worker_out.log` с `вставлено в БД`

**Плюсы**: не требует изменений в C#-коде, работает сразу.
**Минусы**: хрупкий парсинг, зависит от форматирования строк лога.

### B. Health/status endpoint FakeTickServer (рекомендуемый)

**Где меняем**: [`tests/FakeTickServer/TickGeneratorService.cs`](tests/FakeTickServer/TickGeneratorService.cs) + [`start_loadtest.ps1`](start_loadtest.ps1)

- Добавить в health-ответ FakeTickServer (`GET /health`) поля:
  - `totalTicks` — сколько сгенерировано
  - `uniqueTicks` — сколько уникальных
  - `dupPercent` — текущий процент дублей
- В скрипте после завершения генерации сделать `GET /health` и вывести эти поля

### C. Stats endpoint Worker (для будущих прогонов)

**Где меняем**: Worker + [`start_loadtest.ps1`](start_loadtest.ps1)

- `/stats` endpoint на Worker, возвращающий `processedCount` (уже есть в `MarketDataProcessor.GetProcessedCountAsync()`)

## 4. Рекомендуемый план

### Шаг 1: FakeTickServer — расширить health-ответ

Добавить в [`tests/FakeTickServer/TickGeneratorService.cs`](tests/FakeTickServer/TickGeneratorService.cs) счётчики в health:

```csharp
// В классе TickGeneratorService уже есть поля:
//   _totalTicks, _uniqueTicks (используются для лога)
// Добавить в health-endpoint их вывод

public object GetHealthStatus() => new
{
    status = isRunning ? "running" : "completed",
    totalTicks = Interlocked.Read(ref _totalTicks),
    uniqueTicks = Interlocked.Read(ref _uniqueTicks),
    dupPercent = DupPercent,
    isLimitReached = Volatile.Read(ref _isLimitReached)
};
```

### Шаг 2: Worker — добавить `/stats` endpoint (или использовать `/health`)

В [`Worker.cs`](src/MarketDataCollector.Workers/MarketDataCollector.Worker/Worker.cs) `/health` уже возвращает `processedCount`. Может быть, достаточно парсить лог.

### Шаг 3: start_loadtest.ps1 — вывод статистики

После шага 6 добавить:

```powershell
# Парсинг итоговой статистики из health-ответа
$healthResponse = Invoke-RestMethod -Uri "http://localhost:5000/health" -ErrorAction SilentlyContinue
if ($healthResponse.uniqueTicks) {
    Write-Host "  Уникальных тиков сгенерировано: $($healthResponse.uniqueTicks)"
    Write-Host "  Дублей: $($healthResponse.totalTicks - $healthResponse.uniqueTicks)"
}
```

После шага 7 добавить:

```powershell
# Из итоговой строки worker лога
$workerLog = Get-Content "./traces/worker_out.log"
$finalLine = $workerLog | Select-String "Обработчик рыночных данных остановлен"
if ($finalLine) {
    $inserted = [regex]::Match($finalLine, 'вставлено в БД:\s*(\d+)').Groups[1].Value
    $received = [regex]::Match($finalLine, 'получено из канала:\s*(\d+)').Groups[1].Value
    Write-Host "  Получено из канала: $received"
    Write-Host "  Вставлено в БД: $inserted"
}
```

## 5. Итоговый вывод в консоль (пример)

После изменений в конце loadtest-прогона будет выводиться:

```
========================================
  Статистика тиков
========================================
  Всего сгенерировано:              1 700 000
  Уникальных (FakeServer):          1 648 817
  Дублей (3%):                         51 183
  -----------------------------------------
  Получено из канала:               1 457 500
  Дропнуто каналом (DropOldest):       242 500 (14.3%)
  Вставлено в БД (уникальных):      1 413 687
  Отсеяно DeduplicationCache:         ~43 813
  Отсеяно ON CONFLICT DO NOTHING:         ~95
  -----------------------------------------
  Уникальность записи в БД:             85.7%
```

## 6. Файлы для изменений

| Файл | Изменение |
|------|-----------|
| [`tests/FakeTickServer/TickGeneratorService.cs`](tests/FakeTickServer/TickGeneratorService.cs) | +поля `totalTicks`, `uniqueTicks` в health-ответ |
| [`start_loadtest.ps1`](start_loadtest.ps1) | +блок вывода статистики после шагов 6 и 7 |
