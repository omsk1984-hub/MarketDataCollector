# Анализ: Почему ExponentialReconnectStrategy не использует Polly

## Текущая архитектура

### Где Polly УЖЕ используется
- [`SubscriptionManager`](src/MarketDataCollector.Core/Clients/SubscriptionManager.cs) — использует `Polly.WaitAndRetryAsync` для повторных попыток подписки с конечным числом retry (`MaxSubscribeRetries = 3`).

### Где Polly НЕ используется (предмет вопроса)
- [`ExponentialReconnectStrategy`](src/MarketDataCollector.Core/Clients/ExponentialReconnectStrategy.cs) — самодельная реализация `IReconnectStrategy` с ручным расчётом экспоненциальной задержки.
- [`BaseWebSocketClient.RunBackgroundRecoveryLoopAsync()`](src/MarketDataCollector.Core/Clients/BaseWebSocketClient.cs:291) — фоновый цикл восстановления соединения, который вызывает `IReconnectStrategy` вручную.

## Почему текущий подход НЕ использует Polly — анализ

### 1. Разные паттерны использования

**Polly** (в `SubscriptionManager`) решает задачу: *"выполнить операцию, и если она упала — повторить N раз с задержкой"*. Это конечный, предсказуемый набор попыток.

**Фоновый цикл восстановления** в `BaseWebSocketClient` решает принципиально другую задачу:

```
состояние: Отключён → Попытка подключения → Успех → Мониторинг соединения
                                                      ↓
                                              Соединение разорвано
                                                      ↓
                                              Попытка переподключения
                                                      ↓
                                              Успех / Неудача (цикл)
```

Это **конечный автомат**, а не retry одной операции:
- После успешного подключения цикл не завершается, а переходит в режим мониторинга
- При разрыве соединения цикл снова пытается подключиться
- Количество попыток не ограничено (пока не отменён `CancellationToken`)
- Между попытками может пройти успешное подключение, которое сбрасывает счётчик попыток

### 2. IReconnectStrategy — чистая абстракция

Интерфейс [`IReconnectStrategy`](src/MarketDataCollector.Core/Interfaces/IReconnectStrategy.cs) содержит три метода:

| Метод | Назначение |
|-------|------------|
| `GetDelay(int attempt)` | Вычислить задержку перед следующей попыткой |
| `ShouldRetry(int attempt)` | Решить, стоит ли продолжать попытки |
| `Reset()` | Сбросить состояние после успешного подключения |

Это **чистая стратегия** (GoF Strategy pattern), которая:
- Отделяет алгоритм расчёта задержки от логики цикла восстановления
- Позволяет легко подменять стратегию (Exponential, Linear, Fixed, Fibonacci и т.д.)
- Легко тестируется изолированно
- Не требует внешних зависимостей

### 3. Что дало бы использование Polly?

Если бы мы заменили `ExponentialReconnectStrategy` на Polly, мы получили бы:

**Плюсы:**
- Меньше самописного кода
- Встроенный jitter (случайное отклонение задержки для избежания Thundering Herd)
- Возможность использовать `CircuitBreaker` для временной блокировки повторных попыток при большом количестве ошибок
- Единый стиль с `SubscriptionManager`

**Минусы:**
- Polly не поддерживает "бесконечный" цикл с мониторингом состояния "из коробки"
- Пришлось бы костылить: либо `RetryPolicy` с огромным `retryCount`, либо внешний `while(true)` + Polly внутри
- Потеря чистой абстракции `IReconnectStrategy` — привязка к конкретной библиотеке
- Усложнение тестирования (придётся мокать Polly pipeline)
- Polly v8 `ResiliencePipeline` не имеет встроенного "сброса после успеха" — пришлось бы создавать новый pipeline после каждого успешного подключения

### 4. Реалистичные сценарии улучшения

#### Вариант A: Оставить как есть (рекомендуется)
Текущая архитектура корректна:
- `IReconnectStrategy` — чистая стратегия, легко тестируется
- `ExponentialReconnectStrategy` — 30 строк кода, прозрачная логика
- `RunBackgroundRecoveryLoopAsync` — конечный автомат, который не вписывается в модель Polly
- Polly уже используется там, где это уместно (подписка с конечным числом retry)

#### Вариант B: Добавить jitter в ExponentialReconnectStrategy
Можно улучшить существующую стратегию, добавив случайное отклонение:

```csharp
public TimeSpan GetDelay(int attempt)
{
    var baseDelaySeconds = Math.Min(
        _options.ReconnectDelay.TotalSeconds * Math.Pow(2, attempt - 1),
        _options.MaxReconnectDelay.TotalSeconds);
    
    // Jitter: ±25% от baseDelay
    var jitter = Random.Shared.NextDouble() * 0.5 - 0.25; // [-0.25, 0.25]
    var delaySeconds = baseDelaySeconds * (1 + jitter);
    
    return TimeSpan.FromSeconds(Math.Max(1, delaySeconds));
}
```

#### Вариант C: Использовать Polly только для ConnectAsync
Можно обернуть вызов `ConnectAsync` внутри `RunBackgroundRecoveryLoopAsync` в Polly pipeline, но внешний цикл мониторинга останется:

```csharp
// Внутри RunBackgroundRecoveryLoopAsync:
await _resiliencePipeline.ExecuteAsync(
    ct => ConnectAsync(ct), cancellationToken);
```

Но это добавляет зависимость и сложность без существенной выгоды.

## Вывод

**Текущая реализация без Polly — осознанное и корректное решение.**

1. `ExponentialReconnectStrategy` реализует паттерн **Strategy**, а не Retry — это разные уровни абстракции.
2. Фоновый цикл восстановления — это **конечный автомат** с мониторингом состояния, а не retry одной операции.
3. Polly уже используется в проекте там, где это уместно — в `SubscriptionManager` для retry подписки.
4. Если требуется улучшение, наиболее прагматичным будет **добавление jitter** в `ExponentialReconnectStrategy`, а не замена на Polly.

## Рекомендация

Оставить текущую архитектуру. При желании улучшить — добавить jitter в `ExponentialReconnectStrategy.GetDelay()`.