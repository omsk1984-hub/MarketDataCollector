# План: задержка 1 секунда перед генерацией тиков в FakeTickServer

## Цель
После подключения клиента к WebSocket-сокету FakeTickServer должен начинать
генерировать и отправлять тики только через 1 секунду.

## Анализ
В [`TickGeneratorService.ExecuteAsync`](src/../tests/FakeTickServer/TickGeneratorService.cs:166)
генерация стартует сразу после ожидания первого клиента:

```
_logger.LogInformation("Первый клиент подключился, запуск генерации тиков");

// Таймер для логирования статистики раз в 5 секунд
using var statsTimer = new PeriodicTimer(TimeSpan.FromSeconds(5));
var stopwatch = System.Diagnostics.Stopwatch.StartNew();
```

Stopwatch запускается в тот же момент, когда появляется клиент. Чтобы тики
начали генерироваться только через 1 секунду, нужно вставить `Task.Delay(1000)`
между подключением клиента и запуском stopwatch (до него), чтобы RPS-контроль
корректно отсчитывался от реального старта генерации.

## Шаги
1. В `ExecuteAsync` сразу после логирования «Первый клиент подключился» добавить
   паузу 1 секунду с уважением к cancellation token:
   ```csharp
   // Пауза 1 секунда перед началом генерации, чтобы клиент успел "осмотреться"
   await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
   ```
2. Запустить stopwatch после паузы (как сейчас).
3. Проверить сборку проекта FakeTickServer.

## Риски
- `Task.Delay` с `stoppingToken` — если сервер остановят во время паузы, возникнет
  `OperationCanceledException`, но он уже перехватывается в блоке `catch (OperationCanceledException)`
  ниже, поэтому завершение будет корректным.
