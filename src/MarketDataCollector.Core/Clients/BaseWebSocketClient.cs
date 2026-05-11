using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.WebSockets;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Базовый координатор WebSocket-клиента для бирж.
/// Делегирует управление соединением, приём сообщений и подписку специализированным компонентам.
/// </summary>
/// <remarks>
/// <para><b>SRP:</b> Класс выступает координатором, а не монолитом. Вся логика вынесена в:</para>
/// <list type="bullet">
///   <item><see cref="IWebSocketConnectionManager"/> — управление соединением</item>
///   <item><see cref="IWebSocketMessageReceiver"/> — цикл приёма сообщений</item>
///   <item><see cref="IReconnectStrategy"/> — стратегия переподключения</item>
///   <item><see cref="ISubscriptionManager"/> — логика подписки</item>
/// </list>
/// <para><b>Наследование:</b> Производные классы должны переопределить <see cref="SubscribeToTickerAsync"/>
/// для отправки специфичных сообщений подписки и <see cref="ProcessMessageAsync"/> для парсинга сообщений.</para>
/// <para><b>Управление ресурсами:</b> Приоритет за <see cref="IAsyncDisposable"/>. Синхронный <see cref="Dispose"/>
/// реализован для обратной совместимости, но может вызывать deadlock в синхронном контексте.</para>
/// </remarks>
public abstract class BaseWebSocketClient : IExchangeWebSocketClient, IAsyncDisposable
{
    private readonly IWebSocketConnectionManager _connectionManager;
    private readonly IWebSocketMessageReceiver _messageReceiver;
    private readonly IReconnectStrategy _reconnectStrategy;
    private ISubscriptionManager? _subscriptionManager;
    private readonly WebSocketClientOptions _options;
    protected readonly ILogger<BaseWebSocketClient> _logger;

    private Task? _backgroundRecoveryTask;
    private CancellationTokenSource? _backgroundRecoveryCts;
    private CancellationTokenSource? _receiveLoopCts;
    private Task? _receiveLoopTask;
    private readonly object _backgroundLock = new();
    private bool _disposed;

    /// <inheritdoc />
    public bool IsConnected => _connectionManager.IsConnected;

    /// <inheritdoc />
    public string ExchangeName { get; }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public string Symbol { get; }

    /// <inheritdoc />
    public event EventHandler<string> MessageReceived = null!;

    /// <inheritdoc />
    public event EventHandler Connected = null!;

    /// <inheritdoc />
    public event EventHandler Disconnected = null!;

    /// <inheritdoc />
    public event EventHandler<Exception> ErrorOccurred = null!;

    /// <summary>
    /// Создаёт экземпляр базового WebSocket-клиента.
    /// </summary>
    /// <param name="uri">Адрес WebSocket-сервера.</param>
    /// <param name="exchangeName">Имя биржи.</param>
    /// <param name="symbol">Торговая пара.</param>
    /// <param name="connectionManager">Менеджер соединения.</param>
    /// <param name="messageReceiver">Приёмник сообщений.</param>
    /// <param name="reconnectStrategy">Стратегия переподключения.</param>
    /// <param name="options">Параметры конфигурации.</param>
    /// <param name="logger">Логгер.</param>
    protected BaseWebSocketClient(
        Uri uri,
        string exchangeName,
        string symbol,
        IWebSocketConnectionManager connectionManager,
        IWebSocketMessageReceiver messageReceiver,
        IReconnectStrategy reconnectStrategy,
        IOptions<WebSocketClientOptions> options,
        ILogger<BaseWebSocketClient> logger)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _messageReceiver = messageReceiver ?? throw new ArgumentNullException(nameof(messageReceiver));
        _reconnectStrategy = reconnectStrategy ?? throw new ArgumentNullException(nameof(reconnectStrategy));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        ExchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
        Name = $"{exchangeName}_{symbol}";
        Symbol = symbol ?? throw new ArgumentNullException(nameof(symbol));

        // Подписываемся на событие изменения состояния соединения
        _connectionManager.StateChanged += OnConnectionStateChanged;
    }

    /// <summary>
    /// Устанавливает менеджер подписки после создания клиента.
    /// Используется фабрикой для разрешения циклической зависимости между клиентом и SubscriptionManager.
    /// </summary>
    /// <param name="subscriptionManager">Менеджер подписки.</param>
    public void SetSubscriptionManager(ISubscriptionManager subscriptionManager)
    {
        _subscriptionManager = subscriptionManager ?? throw new ArgumentNullException(nameof(subscriptionManager));
    }

    /// <inheritdoc />
    public virtual async Task ConnectAsync(CancellationToken cancellationToken)
    {
        if (IsConnected)
        {
            _logger.LogDebug("{Name}: Соединение уже установлено — пропуск.", Name);
            return;
        }

        _logger.LogInformation("{Name}: Подключение к бирже {Exchange}...", Name, ExchangeName);

        await _connectionManager.ConnectAsync(GetWebSocketUri(), cancellationToken);

        OnConnected();
        await StartReceiveLoopAsync(cancellationToken);
        
        if (_subscriptionManager != null)
        {
            await _subscriptionManager.SubscribeWithRetryAsync(Symbol, cancellationToken);
        }

        _logger.LogInformation("{Name}: Подключение завершено, подписка оформлена.", Name);
    }

    /// <summary>
    /// Возвращает WebSocket URI для подключения.
    /// Может быть переопределён в наследниках для динамической генерации URI.
    /// </summary>
    protected virtual Uri GetWebSocketUri()
    {
        // По умолчанию используем базовый URI из конструктора.
        // Наследники могут переопределить для специфичной логики.
        throw new InvalidOperationException(
            $"{nameof(GetWebSocketUri)} должен быть переопределён или URI должен передаваться в конструктор напрямую.");
    }

    /// <inheritdoc />
    public virtual Task StartAsync(CancellationToken cancellationToken)
    {
        lock (_backgroundLock)
        {
            if (_backgroundRecoveryTask != null && !_backgroundRecoveryTask.IsCompleted)
            {
                _logger.LogDebug("{Name}: Фоновый цикл восстановления уже запущен.", Name);
                return Task.CompletedTask;
            }

            _backgroundRecoveryCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _backgroundRecoveryTask = RunBackgroundRecoveryLoopAsync(_backgroundRecoveryCts.Token);
        }

        _logger.LogInformation("{Name}: Фоновый цикл восстановления запущен.", Name);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public virtual async Task StopAsync(CancellationToken cancellationToken)
    {
        Task? backgroundTask;
        CancellationTokenSource? backgroundCts;

        lock (_backgroundLock)
        {
            backgroundTask = _backgroundRecoveryTask;
            backgroundCts = _backgroundRecoveryCts;
            _backgroundRecoveryTask = null;
            _backgroundRecoveryCts = null;
        }

        if (backgroundCts != null)
        {
            backgroundCts.Cancel();
            try
            {
                if (backgroundTask != null && !backgroundTask.IsCompleted)
                    await backgroundTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogDebug("{Name}: Остановка отменена по токену.", Name);
            }
            backgroundCts.Dispose();
        }

        _logger.LogInformation("{Name}: Фоновый цикл восстановления остановлен.", Name);
    }

    /// <inheritdoc />
    public virtual async Task DisconnectAsync(CancellationToken cancellationToken)
    {
        if (!IsConnected)
            return;

        try
        {
            await _connectionManager.DisconnectAsync(cancellationToken);
            OnDisconnected();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "{Name}: Ошибка при отключении.", Name);
            OnErrorOccurred(ex);
            throw;
        }
        finally
        {
            StopReceiveLoop();
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Базовая реализация вызывает <see cref="SubscribeToTickerAsync"/> с переданным символом.
    /// Производные классы могут переопределить этот метод для специфичных форматов сообщений.
    /// </remarks>
    public virtual Task SubscribeToTicker(string symbol, CancellationToken cancellationToken)
    {
        var task = SubscribeToTickerAsync(symbol, cancellationToken);
        _logger.LogInformation("Успешно подписались на тикер {Symbol} - {ExchangeName}.", symbol, ExchangeName);
        return task;
    }

    /// <summary>
    /// Отправляет сообщение подписки на тикер.
    /// Вызывается автоматически при каждом подключении.
    /// </summary>
    /// <param name="symbol">Символ для подписки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <remarks>
    /// По умолчанию ничего не делает (для бирж с подпиской в URL).
    /// Переопределите этот метод для отправки специфичных сообщений подписки.
    /// </remarks>
    protected virtual Task SubscribeToTickerAsync(string symbol, CancellationToken cancellationToken)
    {
        // По умолчанию ничего не делает (для бирж с подпиской в URL)
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public virtual Task SendAsync(string message, CancellationToken cancellationToken)
    {
        return _connectionManager.SendAsync(message, cancellationToken);
    }

    /// <summary>
    /// Обрабатывает полученное сообщение.
    /// </summary>
    /// <param name="message">Текст сообщения.</param>
    /// <remarks>
    /// Переопределите этот метод в производных классах для парсинга специфичных форматов сообщений.
    /// </remarks>
    protected internal virtual Task ProcessMessageAsync(string message)
    {
        // По умолчанию ничего не делает — наследники переопределяют
        return Task.CompletedTask;
    }

    private async Task StartReceiveLoopAsync(CancellationToken cancellationToken)
    {
        // Отменяем предыдущий ReceiveLoop, если он есть
        StopReceiveLoop();

        _receiveLoopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _receiveLoopTask = _messageReceiver.StartReceiveLoopAsync(
            processMessage: ProcessMessageAsync,
            onMessageReceived: OnMessageReceived,
            onError: OnErrorOccurred,
            cancellationToken: _receiveLoopCts.Token);

        await Task.CompletedTask; // Запускаем в фоне
    }

    private void StopReceiveLoop()
    {
        _receiveLoopCts?.Cancel();
        _receiveLoopCts?.Dispose();
        _receiveLoopCts = null;
        _receiveLoopTask = null;
    }

    private async Task RunBackgroundRecoveryLoopAsync(CancellationToken cancellationToken)
    {
        int reconnectAttempt = 0;

        _logger.LogDebug("{Name}: Фоновый цикл восстановления запущен.", Name);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAsync(cancellationToken);
                reconnectAttempt = 0;
                _reconnectStrategy.Reset();

                // Ждём, пока соединение активно
                while (IsConnected && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(1000, cancellationToken);
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("{Name}: Соединение разорвано, начинаем переподключение...", Name);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                reconnectAttempt++;

                if (!_reconnectStrategy.ShouldRetry(reconnectAttempt))
                {
                    _logger.LogError("{Name}: Исчерпаны попытки переподключения.", Name);
                    break;
                }

                var delay = _reconnectStrategy.GetDelay(reconnectAttempt);
                _logger.LogWarning(ex,
                    "{Name}: Ошибка подключения (попытка {Attempt}). Повтор через {Delay}s...",
                    Name, reconnectAttempt, delay.TotalSeconds);

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        _logger.LogDebug("{Name}: Фоновый цикл восстановления завершён.", Name);
    }

    private void OnConnectionStateChanged(object? sender, WebSocketState state)
    {
        _logger.LogTrace("{Name}: Состояние соединения изменилось на {State}.", Name, state);
    }

    /// <summary>
    /// Вызывается при получении сообщения.
    /// </summary>
    /// <param name="message">Текст сообщения.</param>
    protected internal virtual void OnMessageReceived(string message)
    {
        MessageReceived?.Invoke(this, message);
    }

    /// <summary>
    /// Вызывается при успешном подключении.
    /// </summary>
    protected internal virtual void OnConnected()
    {
        _logger.LogInformation("{Name}: Подключено.", Name);
        Connected?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>
    /// Вызывается при отключении.
    /// </summary>
    protected internal virtual void OnDisconnected()
    {
        _logger.LogInformation("{Name}: Отключено.", Name);
        Disconnected?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>
    /// Вызывается при возникновении ошибки.
    /// </summary>
    /// <param name="ex">Исключение.</param>
    protected internal virtual void OnErrorOccurred(Exception ex)
    {
        _logger.LogError(ex, "{Name}: Ошибка.", Name);
        ErrorOccurred?.Invoke(this, ex);
    }

    #region IDisposable / IAsyncDisposable

    /// <summary>
    /// Синхронное освобождение ресурсов.
    /// Внимание: может вызвать deadlock в средах с синхронным контекстом (например, SynchronizationContext).
    /// Рекомендуется использовать <see cref="DisposeAsync"/>.
    /// </summary>
    public virtual void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            StopAsync(CancellationToken.None).Wait(_options.DisposeTimeout);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "{Name}: Ошибка при синхронной остановке.", Name);
        }

        DisposeCore();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Асинхронное освобождение ресурсов. Приоритетный метод очистки.
    /// </summary>
    public virtual async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await StopAsync(CancellationToken.None);
        DisposeCore();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    private void DisposeCore()
    {
        StopReceiveLoop();
        _backgroundRecoveryCts?.Cancel();
        _backgroundRecoveryCts?.Dispose();
        _connectionManager.StateChanged -= OnConnectionStateChanged;
        (_connectionManager as IDisposable)?.Dispose();
    }

    #endregion
}
