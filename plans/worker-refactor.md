# План рефакторинга Worker.cs

## Проблема
Цикл `while (!stoppingToken.IsCancellationRequested)` в `ExecuteAsync` избыточен:
- При нормальной работе выполняется 1 раз, затем блокируется на `Task.Delay(Timeout.Infinite)`
- Логика перезапуска при ошибках работает, но делает код сложнее для понимания

## Цель
Убрать цикл `while`, сохранив:
1. Автоматический перезапуск при ошибках с задержкой 30 сек
2. Корректную остановку по токену отмены
3. Health-check каждые 30 сек
4. Остановку клиентов и процессора в finally

## Решение

Вынести логику запуска и перезапуска в отдельный метод `RunWithRecoveryAsync`, который будет вызываться из `ExecuteAsync` и сам управлять перезапусками.

### Новая структура

```
ExecuteAsync(stoppingToken)
└── RunWithRecoveryAsync(stoppingToken) — рекурсивный перезапуск при ошибках
    ├── InitializeServices()
    ├── StartClients()
    ├── StartProcessor()
    ├── HealthCheck loop
    └── Cleanup (finally)
```

## Шаги реализации

1. Создать метод `RunWithRecoveryAsync` с логикой запуска/перезапуска
2. Упростить `ExecuteAsync` до вызова `RunWithRecoveryAsync`
3. Проверить компиляцию
