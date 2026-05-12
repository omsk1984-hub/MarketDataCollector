## 5. Правила и шаблоны для написания новых тестов

Каждый новый тестовый класс и тестовый метод должны следовать установленному стилю диагностики и таймаутов. Это обеспечивает единообразие и предотвращает появление неотловленных висящих тестов.

### 5.1. Обязательные компоненты для нового тестового класса

1. **Конструктор с `ITestOutputHelper`** — всегда добавлять параметр `ITestOutputHelper output` в конструктор
2. **Поле `_output`** — сохранять helper в приватное поле `private readonly ITestOutputHelper _output;`
3. **Диагностический вывод** — каждый **async** тест должен начинаться с `_output.WriteLine($"=== Running: {nameof(TestMethodName)} ===");`
4. **Таймаут на async-тестах** — каждый **async** тест должен иметь `[Fact(Timeout = <ms>)]`

### 5.2. Определение уровня таймаута

| Риск | Таймаут | Категории тестов |
|------|---------|-----------------|
| 🔴 Высокий | `Timeout = 5000` (5 сек) | WebSocket-клиенты, Receive loop, Subscription, Connection Manager, BinanceWebSocketClient |
| 🟡 Средний | `Timeout = 10000` (10 сек) | Repository (EF Core InMemory), DataStorage, MarketDataProcessor |
| 🟢 Низкий | без таймаута | Синхронные тесты (не `async Task`), MonitoringService, WebSocketClientFactory |

### 5.3. Шаблон нового тестового класса

```csharp
using Xunit;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Category;

public class NewServiceTests
{
    private readonly ITestOutputHelper _output;
    // ... другие поля (mocks, сервисы и т.д.)

    public NewServiceTests(ITestOutputHelper output)
    {
        _output = output;
        // ... существующая инициализация ...
    }

    [Fact(Timeout = 5000)]  // 🔴 высокий риск — WebSocket/сеть
    public async Task SomeAsyncMethod_ShouldDoSomething_Successfully()
    {
        _output.WriteLine($"=== Running: {nameof(SomeAsyncMethod_ShouldDoSomething_Successfully)} ===");

        // Arrange
        // Act
        // Assert
    }

    [Fact]  // 🟢 синхронный — без таймаута
    public void SomeSyncMethod_ShouldReturnCorrectResult()
    {
        // синхронные тесты не требуют таймаута
    }
}
```

### 5.4. Правила для новых тестов

| # | Правило | Пояснение |
|---|---------|-----------|
| 1 | **Все async-тесты** получают `[Fact(Timeout = ...)]` | Если тест зависнет, TimeoutException укажет имя теста |
| 2 | **Синхронные тесты** не получают таймаут | Они выполняются мгновенно и не могут "виснуть" в том же смысле |
| 3 | **ITestOutputHelper** добавляется в конструктор, а не через DI-контейнер | xUnit сам внедряет его при создании тестового класса |
| 4 | **Логгирование через `_output.WriteLine`** только для ключевых точек | Не захламлять вывод, только `"=== Running: {name} ==="` в начале async-теста + при необходимости точки входа/выхода |
| 5 | **Таймаут подбирается по риск-категории** | 5s для WebSocket/сеть, 10s для EF Core InMemory, без таймаута для синхронных |
| 6 | **Не использовать `Thread.Sleep` в тестах** | Заменять на `Task.Delay` с токеном отмены или await завершения |
| 7 | **Не передавать `_output` в тестируемый код** | `ITestOutputHelper` предназначен только для диагностики из теста, не из продакшен-кода |

### 5.5. Чек-лист при добавлении нового тестового файла

- [ ] Добавлен `using Xunit.Abstractions;`
- [ ] Добавлено поле `private readonly ITestOutputHelper _output;`
- [ ] Конструктор принимает `ITestOutputHelper output` и сохраняет в поле
- [ ] Каждый async-тест помечен `[Fact(Timeout = 5000)]` или `[Fact(Timeout = 10000)]`
- [ ] Каждый async-тест начинается с `_output.WriteLine($"=== Running: {nameof(...)} ===");`
- [ ] Синхронные тесты используют `[Fact]` без таймаута
- [ ] В `.csproj` добавлен `<Content Include="xunit.runner.json" CopyToOutputDirectory="PreserveNewest" />` (если ещё не добавлен)

### 5.6. Пример готового теста

```csharp
[Fact(Timeout = 10000)]
public async Task SaveTicksAsync_ShouldPersistTicksToDatabase()
{
    _output.WriteLine($"=== Running: {nameof(SaveTicksAsync_ShouldPersistTicksToDatabase)} ===");

    // Arrange
    var ticks = new List<RawTick> { /* ... */ };

    // Act
    await _repository.AddRangeAsync(ticks);
    await _repository.SaveChangesAsync();

    // Assert
    var saved = await _repository.GetAllAsync();
    Assert.Equal(ticks.Count, saved.Count());
}
```
