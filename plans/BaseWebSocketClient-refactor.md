# План рефакторинга BaseWebSocketClient.cs по принципам SOLID

## 1. Анализ текущих нарушений SOLID

### Single Responsibility Principle (SRP)
Класс совмещает несколько несвязанных обязанностей:
- Управление жизненным циклом соединения (ConnectAsync, DisconnectAsync, StartAsync, StopAsync)
- Цикл приёма и парсинга сообщений (ReceiveLoopAsync, StartReceiveLoopAsync)
- Стратегия переподключения с экспоненциальной задержкой (RunBackgroundRecoveryLoopAsync, ReconnectAsync)
- Встроенная политика повторных попыток на базе Polly (_retryPolicy, subscribeRetryPolicy)
- Логирование через Console.WriteLine (LogAsync)
- Управление ресурсами и отмена задач (Dispose, DisposeAsync, CancellationTokenSource)
- Механизм подписки на тикеры (SubscribeToTicker, SubscribeToTickerAsync, SubscribeToTickerWithRetryAsync)

### Open/Closed Principle (OCP)
- Задержки переподключения (_reconnectDelay, _maxReconnectDelay) и количество попыток захардкожены в полях класса
- Политика Polly создаётся в конструкторе без возможности замены стратегии обработки ошибок
- Логирование привязано к Console, расширение под Microsoft.Extensions.Logging требует изменения базового класса
- Формат сообщений подписки жёстко завязан на переопределение в наследниках без чёткого контракта

### Liskov Substitution Principle (LSP)
- Публичный метод SubscribeToTicker выбрасывает NotImplementedException, что нарушает контракт интерфейса IExchangeWebSocketClient
- Синхронный Dispose вызывает StopAsync(CancellationToken.None).Wait(TimeSpan.FromSeconds(5)), что создаёт риск deadlock при наследовании и в средах с синхронным контекстом
- ProcessMessageAsync имеет пустую реализацию, но в BinanceWebSocketClient ожидается обязательная логика парсинга без явного указания в сигнатуре базового класса

### Interface Segregation Principle (ISP)
- Интерфейс IExchangeWebSocketClient содержит события и методы управления, которые не всегда нужны всем потребителям
- Класс реализует IDisposable и IAsyncDisposable с дублирующейся логикой очистки, что усложняет контракт для клиентов

### Dependency Inversion Principle (DIP)
- Прямые зависимости на конкретные реализации: ClientWebSocket, Polly.Policy, Console, Math.Pow
- Отсутствие абстракций для фабрики сокетов, стратегии переподключения, логирования и управления циклом приёма
- Конфигурация задержек и лимитов передаётся через поля, а не через IOptions или DI-контейнер

## 2. Цели рефакторинга

- Разделить ответственности на специализированные компоненты
- Вынести конфигурацию и стратегии поведения во вне через внедрение зависимостей
- Устранить нарушения контрактов наследования и асинхронногоDispose
- Подготовить класс к покрытию unit-тестами через моки интерфейсов
- Сохранить обратную совместимость для BinanceWebSocketClient и текущей архитектуры Worker/Factory

## 3. Пошаговый план рефакторинга

### Шаг 1. Выделение ответственностей (SRP)
- Создать IWebSocketConnectionManager: управление состоянием, ConnectAsync, DisconnectAsync, проверка IsConnected
- Создать IWebSocketMessageReceiver: цикл приёма, сборка фрагментов, вызов ProcessMessageAsync
- Создать IReconnectStrategy: расчёт задержек, лимиты попыток, экспоненциальный backoff с cap
- Создать ISubscriptionManager: отправка сообщений подписки, обработка retry-логики подписки
- BaseWebSocketClient оставить как координатор, связывающий компоненты и управляющий фоновым циклом восстановления

### Шаг 2. Устранение жёсткой связанности (OCP)
- Заменить захардкоженные задержки на настраиваемые параметры через WebSocketClientOptions (IOptions)
- Вынести Polly-политику в отдельный сервис IResilienceProvider или использовать Microsoft.Extensions.Resilience
- Заменить Console.WriteLine на ILogger<BaseWebSocketClient> через конструктор
- Сделать стратегии поведения заменяемыми без модификации базового класса

### Шаг 3. Исправление контрактов наследования (LSP)
- Изменить SubscribeToTicker на virtual с безопасной реализацией по умолчанию или убрать NotImplementedException
- ProcessMessageAsync оставить virtual, но добавить XML-документацию с требованием переопределения в наследниках
- Исправить Dispose: убрать синхронный Wait, сделать IAsyncDisposable основным контрактом, IDisposable реализовать через вызов DisposeAsync().AsTask().Wait() только при необходимости с пометкой Obsolete или явным предупреждением
- Гарантировать, что базовый класс не требует обязательного переопределения методов, ломающих поведение при вызове через базовый тип

### Шаг 4. Упрощение интерфейсов (ISP)
- Оставить в IExchangeWebSocketClient только методы и события, необходимые всем реализациям
- Разделить IDisposable и IAsyncDisposable, оставив приоритет за асинхронной версией
- При необходимости вынести события в отдельный интерфейс IWebSocketEventPublisher

### Шаг 5. Внедрение зависимостей (DIP)
- Внедрить через конструктор:
  - ILogger<BaseWebSocketClient>
  - IReconnectStrategy
  - IWebSocketConnectionManager
  - IWebSocketMessageReceiver
  - ISubscriptionManager
  - WebSocketClientOptions
- Убрать прямые вызовы new ClientWebSocket(), Math.Pow(), Console.WriteLine
- Зависеть от абстракций, а не от конкретных библиотек

## 4. Предлагаемая структура после рефакторинга

BaseWebSocketClient выступает координатором:
- Принимает зависимости через конструктор
- Управляет CancellationTokenSource жизненного цикла
- Запускает фоновый цикл восстановления через IReconnectStrategy
- Делегирует приём сообщений в IWebSocketMessageReceiver
- Делегирует подписку в ISubscriptionManager
- Логирует через ILogger

Интерфейсы:
- IWebSocketConnectionManager: ConnectAsync, DisconnectAsync, IsConnected, StateChanged
- IWebSocketMessageReceiver: StartReceiveLoopAsync, StopReceiveLoopAsync, ProcessMessageAsync
- IReconnectStrategy: GetDelayAsync(int attempt), ShouldRetry(int attempt), Reset()
- ISubscriptionManager: SubscribeAsync(string symbol, CancellationToken)
- IWebSocketFactory: CreateClientAsync(Uri, CancellationToken)

## 5. Этапы внедрения

1. Подготовка абстракций и настроек
   - Создать интерфейсы и дефолтные реализации
   - Добавить класс WebSocketClientOptions с параметрами задержек, лимитов,容量的 канала
   - Настроить регистрацию в Program.cs через builder.Services.Configure

2. Извлечение логики из BaseWebSocketClient
   - Перенести управление сокетом в WebSocketConnectionManager
   - Вынести цикл приёма и сборку фрагментов в WebSocketMessageReceiver
   - Заменить хардкод задержек на IReconnectStrategy
   - Вынести логику подписки в SubscriptionManager

3. Исправление контрактов
   - Убрать NotImplementedException из SubscribeToTicker
   - Исправить Dispose/DisposeAsync, убрать синхронные ожидания
   - Добавить XML-документацию с указанием требований к наследникам

4. Внедрение зависимостей
   - Переписать конструктор BaseWebSocketClient
   - Обновить WebSocketClientFactory для передачи зависимостей через ServiceProvider
   - Настроить Options и ILogger в DI-контейнере

5. Адаптация производных классов
   - Проверить BinanceWebSocketClient на совместимость с новыми виртуальными методами
   - При необходимости адаптировать вызовы базовых методов
   - Убедиться в корректной работе событий и парсинга

6. Тестирование и код-ревью
   - Написать unit-тесты для стратегий, менеджеров и фабрик
   - Провести интеграционное тестирование переподключений и обработки ошибок
   - Проверить отсутствие утечек памяти, deadlock и корректную работу CancellationToken

## 6. Ожидаемый результат

- Размер BaseWebSocketClient сократится на 40-50 процентов за счёт вынесения логики в специализированные компоненты
- Появится возможность заменять стратегии переподключения, логирования и подписки без изменения базового класса
- Логирование станет настраиваемым и интегрированным с Microsoft.Extensions.Logging
- Наследники смогут безопасно переопределять поведение без риска нарушения контрактов и deadlock
- Код станет полностью тестируемым через моки интерфейсов
- Соответствие современным практикам NET 8, рекомендациям Microsoft по асинхронному программированию и управлению ресурсами
- Сохранение обратной совместимости с текущей архитектурой Worker, Factory и BinanceWebSocketClient