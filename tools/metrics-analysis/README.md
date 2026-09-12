# Скрипты анализа метрик MarketDataCollector

Переиспользуемые PowerShell-скрипты для разбора `counters_<ts>.csv` (формат OpenTelemetry-Prometheus экспортера). Применяются в рамках skill `metrics-analysis`.

> ⚠️ **Важно про парсинг больших CSV**: строки имеют формат `"ts","metric","labels","type","value","desc"`. Поле `labels` может содержать вложенные двойные кавычки и запятые, поэтому **`Split('","')` НЕ надёжен** — он смещает индексы полей. Единственный достоверный способ — **regex** с якорем на закрывающую кавычку каждого поля:
> ```regex
> ^"([^"]*)","([^"]*)","((?:[^"]|"")*)","([^"]*)","([^"]*)"   (group5 = value)
> ```
> Также **нельзя** запускать inline-команды через `execute_command` с кавычками — `cmd /c` и оболочка их портят (кавычки `'"'` удаляются). Всегда запускай скрипт через `powershell -NoProfile -File <script>.ps1 <counters.csv>` с выводом в файл, а не в stdout.

## Состав

- [`summary_reader.ps1`](summary_reader.ps1) — сводка прогона: число строк/сэмплов, первый/последний timestamp, список уникальных метрик, первый→последний сэмпл по каждому ряду (только не histogram-bucket). Результат: `plans/_metrics_out.txt`.
- [`detail_series.ps1`](detail_series.ps1) — детальные ряды по интересующим метрикам с полноценным парсингом labels (GC по поколениям, WS по символам, дедупликация, каналы). Результат: `plans/_metrics_detail.txt`.
- [`batch_duration_stats.ps1`](batch_duration_stats.ps1) — статистика длительности записи батчей: мин/макс/среднее/медиана длительности, распределение `inserted_count`. Результат: `plans/_batch_dur.txt`.
- [`_diag.ps1`](_diag.ps1) — диагностика структуры строки CSV (для отладки парсинга). Результат: `plans/_diag.txt`.

## Использование

```bash
powershell -NoProfile -File tools/metrics-analysis/summary_reader.ps1      traces/counters_<ts>.csv
powershell -NoProfile -File tools/metrics-analysis/detail_series.ps1       traces/counters_<ts>.csv
powershell -NoProfile -File tools/metrics-analysis/batch_duration_stats.ps1 traces/counters_<ts>.csv
```

Каждый скрипт читает CSV из аргумента `$args` и пишет результат в соответствующий `*_out/detail/dur.txt` в `plans/`, а в терминал выводит только строку `WROTE ...`.

## Анализ вручную (быстрый осмотр больших CSV)

Прямое чтение фрагментов через `read_file` (offset/limit) — надёжнее, чем regex-поиск по всему файлу (`search_files` по 10+ MB CSV может давать «0 совпадений» по фактически присутствующим метрикам — глюк индексации больших CSV).