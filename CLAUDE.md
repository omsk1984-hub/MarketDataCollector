# CLAUDE.md

## 🎯 Стек
- **.NET 8, C# 12** — основная платформа
- **Entity Framework Core 8** — ORM для работы с БД
- **PostgreSQL 16** — база данных
- **Docker / Docker Compose** — контейнеризация БД
- **Polly 8** — политики повторных попыток (референс в проекте; фактическая стратегия — собственная реализация `ExponentialReconnectStrategy`)
- **Newtonsoft.Json** — парсинг JSON сообщений бирж
- **Npgsql** — драйвер PostgreSQL для .NET
- **WebSocket (`System.Net.WebSockets`)** — протокол для реального времени
- **xUnit + Moq + FluentAssertions** — модульное тестирование

## 📐 Архитектура
- Вертикальные срезы (Vertical Slices) или Clean Architecture
- SOLID
- Один файл = одна ответственность
- Интерфейсы в `Application/`, реализации в `Infrastructure/`
- DTO через `record`, доменные модели через `class`

## 🧪 Правила кода
- `async/await` до самого низкого уровня
- `ConfigureAwait(false)` только в библиотеках
- Null-check через `ArgumentNullException.ThrowIfNull()`
- Логирование через `ILogger<T>`
- Тесты покрывают happy path + edge cases
- Использовать `dotnet format` перед коммитом

## 🤖 Инструкции для AI
- Не генерируй `Program.cs` целиком, если не просили
- При добавлении NuGet: покажи команду `dotnet add package`
- Всегда указывай `using` явно, не полагайся на implicit
- Если сомневаешься — задай уточняющий вопрос