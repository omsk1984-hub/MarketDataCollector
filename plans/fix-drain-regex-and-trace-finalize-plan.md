# План исправления скриптов сбора метрик

## Контекст / проблема

Прогон `run_all_metrics.ps1` (timestamp `20260731_075117`) отработал с двумя дефектами:

1. **Потерян `allocation_trace_*.nettrace`** (и `.speedscope.json`).
   В `traces/` присутствуют только: `counters_*.csv`, `snapshot_peak_*.gcdump`,
   `snapshot_drained_*.gcdump`, `profiling_report_*.md`.
   В сводном отчёте размеры `nettrace` / `speedscope.json` = `N/A`.
   В логе при этом: `[*] dotnet-trace уже завершён.` — без каких-либо ошибок.

2. **Дренаж канала не детектирован** — фаза `WaitFor-Drain` все 30с выводила
   `/metrics доступен, но backlog не найден`, затем просто истекла по таймеру.
   Второй `gcdump` (DRAINED) снят на **растущем** бэклоге
   (`processor_channel_backlog_count = 31248`, `ticks_dropped_silently = 29924`),
   то есть не после дренажа → сравнение peak/drained некорректно.

---

## Корневые причины

### Причина 1 — сломанный regex дренажа

Файл: [`scripts/common-functions.ps1`](scripts/common-functions.ps1:417)

Текущая строка:
```powershell
if ($resp.Content -match 'processor_channel_backlog_count\s+(\d+)') {
```

Prometheus text format выставляет метрику с лейблами между именем и значением:
```
processor_channel_backlog_count{otel_scope_name="MarketDataCollector"; ...} 8596
```

Regex ищет `processor_channel_backlog_count`, затем пробел и число. Между ними
`{...}` → матч не срабатывает никогда, ветка `else` выводит
`/metrics доступен, но backlog не найден`. Таймер истекает вхолостую.

> Контр-доказательство: метрика присутствует в CSV
> `traces/counters_20260731_075117.csv:990` (значение 8596), т.е. `/metrics` её
> действительно отдаёт — проблема именно в regex.

### Причина 2 — молчаливая потеря nettrace

Файл: [`scripts/common-functions.ps1`](scripts/common-functions.ps1:161)

- [`Start-TraceCollection`](scripts/common-functions.ps1:201) запускает
  `$proc.StandardOutput.ReadToEndAsync()` / `StandardError.ReadToEndAsync()`
  и сохраняет задачи в объект (поля `StdoutTask` / `StderrTask`).
- Эти задачи **нигде не читаются** после завершения процесса.
- Если `dotnet-trace` завершился с ненулевым кодом (отказ attach, конфликт
  профиля, сбой финализации) — информация теряется молча. Скрипт в
  [`Stop-TraceCollection`](scripts/common-functions.ps1:234) просто проверяет
  `HasExited` и выводит «уже завершён».

Дополнительно: в [`collect-all.ps1`](scripts/collect-all.ps1:195) цикл ожидания
прерывается по `$traceJob.Process.HasExited`, но `ExitCode` / stdout / stderr не
проверяются и не логируются.

---

## Правки

### Правка A — починить regex дренажа

Файл: `scripts/common-functions.ps1`, функция `WaitFor-Drain`.

Заменить жёсткий regex на regex, допускающий лейблы:

```powershell
if ($resp.Content -match 'processor_channel_backlog_count(?:\{[^}]*\})?\s+(\d+)') {
    $backlog = [int]$Matches[1]
    ...
}
```

- `(?:\{[^}]*\})?` — необязательная группа лейблов `{...}`.
- `\s+(\d+)` — значение.

Дополнительно (устойчивость): сделать парсинг нечувствительным к порядку, если
в /metrics метрика вдруг переименована, — но для текущего кода достаточно
указанного паттерна.

**Ожидаемый результат:** `WaitFor-Drain` начнёт видеть бэклог, выводить
`Backlog: N` и завершаться досрочно при `backlog == 0` (канал дренирован),
второй `gcdump` будет браться после реального дренажа.

### Правка B — диагностика и сохранение nettrace

Файл: `scripts/common-functions.ps1`, функции `Start-TraceCollection` и
`Stop-TraceCollection`.

1. В `Stop-TraceCollection` после `HasExited` — считать и залогировать
   `ExitCode`, а также содержимое `StdoutTask` / `StderrTask` (к моменту вызова
   они уже завершены):

   ```powershell
   $stdout = ""
   $stderr = ""
   try { $stdout = $TraceProcess.StandardOutput... } catch { }
   try { $stderr = $TraceProcess.StandardError... } catch { }
   ```

   > Поскольку задачи хранятся в объекте `$traceJob` (поле `StdoutTask` /
   > `StderrTask`), их нужно пробросить в `Stop-TraceCollection`. Проще всего:
   > добавить параметр `-TraceJob` и читать `$TraceJob.StdoutTask.GetAwaiter().GetResult()`.

2. Проверка ExitCode:
   - Если `ExitCode -eq 0` и файл `OutputPath` существует и не пуст —
     `dotnet-trace` завершился штатно, файл финализирован.
   - Если `ExitCode -ne 0` или файл отсутствует/пуст — вывести предупреждение
     с текстом `stderr` (например, причина отказа attach). Это позволит
     диагностировать потерю трассы вместо молчаливого «уже завершён».

### Правка C — вызов Stop-TraceCollection с пробросом job и проверкой файла

Файл: `scripts/collect-all.ps1`, шаг 5 (строки 207–209).

- Передать `$traceJob` в `Stop-TraceCollection` (вместо только `$traceJob.Process`).
- После остановки — проверка существования и размера `$traceFile`; если файла
  нет — вывести явное предупреждение `[!] nettrace не создан` и залогировать
  stderr из job.

### Правка D (рекомендуемая) — единая проверка trace-файла в отчёте

Файл: `scripts/collect-all.ps1`, шаг 9 (генерация отчёта).

Уже есть `Test-Path $traceFile` для размеров. Добавить логирование, если
`nettrace` или `speedscope.json` не созданы, чтобы отчёт не показывал `N/A`
без явного предупреждения об аномалии сбора.

---

## Область проверки / риски

- **Файл без лейблов** (fallback): `processor_channel_backlog_count 0` —
  новый regex матчит и без лейблов, регрессии нет.
- **Пустой / отсутствующий stderr**: обработчики `try/catch` вокруг
  `GetAwaiter().GetResult()` — чтение не должно падать.
- **Обратная совместимость** `Stop-TraceCollection`: добавление нового
  параметра `-TraceJob` с дефолтом `$null` не ломает существующие вызовы из
  других скриптов (`scripts/collect-trace.ps1` и т.д.).
- **Перенаправление stdout/stderr** сохраняется (иначе pipe-буфер переполнится
  и dotnet-trace заблокируется) — правка лишь добавляет чтение задач, а не
  отменяет их.

---

## Критерии готовности

После правок повторный прогон `run_all_metrics.ps1` должен:

1. В фазе дренажа выводить `Backlog: N` и завершаться по `backlog == 0`
   (не по таймеру), если канал реально дренирован.
2. Создавать `allocation_trace_<ts>.nettrace` и `allocation_trace_<ts>.speedscope.json`
   с ненулевым размером.
3. В сводном отчёте размеры `nettrace` и `speedscope.json` не равны `N/A`.
4. При сбое dotnet-trace — выводить явное предупреждение с текстом `stderr`
   (вместо молчаливого «уже завершён»).
