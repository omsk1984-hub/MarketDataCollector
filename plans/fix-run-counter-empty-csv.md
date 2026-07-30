# План: Исправление run_counter.ps1 — пустой CSV

## Проблема

Скрипт [`run_counter.ps1`](run_counter.ps1) вызывает `dotnet counters collect`, но файл CSV остаётся пустым (только заголовок). Кастомные метрики приложения (MarketDataTelemetry) не собираются — это ОК, т.к. пользователь хочет только стандартные .NET Runtime/ASP.NET счётчики.

## Корневые причины

1. **Нет HTTP-проверки Worker'а** — Скрипт ждёт 8 секунд, но не проверяет, что Worker реально запустился и отвечает на `/health`.
2. **Нет валидации данных в CSV** — Проверяется только `Test-Path`, но не количество строк.
3. **PID может быть неверным** — `Start-Process -FilePath "dotnet" -ArgumentList "run"` создаёт родительский `dotnet.exe`, реальный Worker — дочерний процесс.

## Исправления

### 1. Добавить HTTP health-check перед запуском counters

После запуска Worker'а (шаг 6) добавить цикл, который опрашивает `http://localhost:5010/health` до получения HTTP 200. Это подтвердит, что Worker полностью инициализирован.

```
Цикл: max 30 попыток, каждые 2 секунды
  GET http://localhost:5010/health
  Если 200 — Worker готов
  Если не 200 или timeout — продолжаем
Если за 60 сек нет ответа — ошибка
```

### 2. Заменить `Start-Sleep -Seconds 8` на реальный health-check

Убрать статический `Start-Sleep -Seconds 8` (строка 108) и заменить на цикл с HTTP-запросом.

### 3. Добавить валидацию CSV после сбора

После остановки counters (шаг 10) добавить проверку: если в файле только 1 строка (заголовок) — предупредить пользователя.

### 4. Добавить вывод PID всех дочерних процессов (диагностика)

Для отладки вывести PID дочерних процессов Worker'а (чтобы понимать, какой PID мониторится).

## Схема изменений

```
До:
  Start-Process Worker → Wait 30s → Sleep 8s → Start counters
  Read-Host → Stop Worker → Wait counters → Check file exists

После:
  Start-Process Worker → Wait 30s → HTTP GET /health loop → Start counters
  Read-Host → Stop Worker → Wait counters → Check file has data rows
```

## Файлы для изменения

- [`run_counter.ps1`](run_counter.ps1) — единственный файл

## Проверка

1. Запустить `run_fake_server.ps1` (если нужно)
2. Запустить `run_counter.ps1`
3. Убедиться, что CSV содержит строки данных (не только заголовок)
4. Убедиться, что скрипт корректно останавливает Worker и counters
