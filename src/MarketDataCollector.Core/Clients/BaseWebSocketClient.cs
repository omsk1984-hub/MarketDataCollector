using MarketDataCollector.Core.Interfaces;
using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;

namespace MarketDataCollector.Core.Clients
{
    public abstract class BaseWebSocketClient : IExchangeWebSocketClient, IAsyncDisposable
    {
        private ClientWebSocket _webSocket;
        private readonly Uri _uri;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private bool _disposed;
        private readonly AsyncRetryPolicy _retryPolicy;
        private Task _receiveLoopTask;
        private CancellationTokenSource _receiveLoopCts;
        
        // Поля для автоматического восстановления
        private Task? _backgroundRecoveryTask;
        private CancellationTokenSource? _backgroundRecoveryCts;
        private readonly TimeSpan _reconnectDelay = TimeSpan.FromSeconds(5);
        private readonly TimeSpan _maxReconnectDelay = TimeSpan.FromSeconds(60);
        private readonly object _backgroundLock = new object();

        public bool IsConnected => Volatile.Read(ref _webSocket)?.State == WebSocketState.Open;
        public string ExchangeName { get; protected set; }
        public string Name { get; protected set; }
        public string Symbol { get; protected set; }

        public event EventHandler<string> MessageReceived = null!;
        public event EventHandler Connected = null!;
        public event EventHandler Disconnected = null!;
        public event EventHandler<Exception> ErrorOccurred = null!;

        protected BaseWebSocketClient(string uri, string exchangeName, string symbol)
        {
            _uri = new Uri(uri);
            ExchangeName = exchangeName;
            Name = $"{exchangeName}_{symbol}";
            Symbol = symbol;
            _cancellationTokenSource = new CancellationTokenSource();
            _webSocket = null;
            _receiveLoopTask = null;
            _receiveLoopCts = null;
            
            // Политика повторных попыток с экспоненциальной задержкой
            _retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount: 5,
                    sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // экспоненциальная задержка 2,4,8,16,32 секунды
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        // Логирование попытки реконнекта
                        OnErrorOccurred(new Exception($"WebSocket reconnect attempt {retryCount} failed. Waiting {timeSpan.TotalSeconds} seconds before next attempt.", exception));
                    });
        }

        public virtual async Task ConnectAsync(CancellationToken cancellationToken)
        {
            if (IsConnected)
                return;

            await _retryPolicy.ExecuteAsync(async () =>
            {
                // Если уже подключились в предыдущей попытке (например, после задержки), проверяем снова
                if (IsConnected)
                    return;

                // Создаём новый ClientWebSocket для каждой попытки
                var ws = new ClientWebSocket();
                await ws.ConnectAsync(_uri, cancellationToken);
                
                // Атомарно заменяем старый сокет, старый диспозим
                var oldWs = Interlocked.Exchange(ref _webSocket, ws);
                oldWs?.Dispose();
                
                OnConnected();
                await StartReceiveLoopAsync();
                await SubscribeToTickerWithRetryAsync(cancellationToken);
            });
        }

        /// <summary>
        /// Запускает автоматическое управление жизненным циклом WebSocket-клиента.
        /// Клиент будет автоматически переподключаться при разрыве соединения.
        /// </summary>
        public virtual Task StartAsync(CancellationToken cancellationToken)
        {
            lock (_backgroundLock)
            {
                if (_backgroundRecoveryTask != null && !_backgroundRecoveryTask.IsCompleted)
                    return Task.CompletedTask;

                _backgroundRecoveryCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _backgroundRecoveryTask = RunBackgroundRecoveryLoopAsync(_backgroundRecoveryCts.Token);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Останавливает автоматическое управление жизненным циклом.
        /// </summary>
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
                    // Ожидаемое поведение при отмене
                }
                backgroundCts.Dispose();
            }
        }

        public virtual async Task DisconnectAsync(CancellationToken cancellationToken)
        {
            var ws = Volatile.Read(ref _webSocket);
            if (ws?.State != WebSocketState.Open)
                return;

            try
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", cancellationToken);
                OnDisconnected();
            }
            catch (Exception ex)
            {
                OnErrorOccurred(ex);
                throw;
            }
            finally
            {
                StopReceiveLoop();
            }
        }

        public virtual async Task SubscribeToTicker(string symbol, CancellationToken cancellationToken)
        {
            // Базовая реализация просто отправляет сообщение подписки
            // Конкретные реализации должны переопределить этот метод для специфичных форматов сообщений
            throw new NotImplementedException("SubscribeToTicker must be implemented in derived classes");
        }

        /// <summary>
        /// Подписывается на тикер. Вызывается автоматически при каждом подключении.
        /// Переопределяется в производных классах для отправки специфичных сообщений подписки.
        /// </summary>
        protected virtual Task SubscribeToTickerAsync(CancellationToken cancellationToken)
        {
            // По умолчанию ничего не делает (для бирж с подпиской в URL)
            return Task.CompletedTask;
        }

        /// <summary>
        /// Подписка на тикер с повторными попытками при ошибке.
        /// </summary>
        private async Task SubscribeToTickerWithRetryAsync(CancellationToken cancellationToken)
        {
            const int maxSubscribeRetries = 3;
            var subscribeRetryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(
                    retryCount: maxSubscribeRetries,
                    sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        OnErrorOccurred(new Exception($"Subscribe attempt {retryCount} failed. Retrying in {timeSpan.TotalSeconds}s.", exception));
                    });

            await subscribeRetryPolicy.ExecuteAsync(async () =>
            {
                await SubscribeToTickerAsync(cancellationToken);
            });
        }

        private async Task StartReceiveLoopAsync()
        {
            // Отменяем предыдущий ReceiveLoop, если он есть
            if (_receiveLoopCts != null)
            {
                _receiveLoopCts.Cancel();
                try
                {
                    if (_receiveLoopTask != null && !_receiveLoopTask.IsCompleted)
                        await _receiveLoopTask;
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    OnErrorOccurred(ex);
                }
                _receiveLoopCts.Dispose();
            }

            // Создаём новый CTS для нового ReceiveLoop
            _receiveLoopCts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token);
            _receiveLoopTask = ReceiveLoopAsync(_receiveLoopCts.Token);
        }

        private void StopReceiveLoop()
        {
            _receiveLoopCts?.Cancel();
            _receiveLoopCts?.Dispose();
            _receiveLoopCts = null;
            _receiveLoopTask = null;
        }

        public virtual async Task SendAsync(string message, CancellationToken cancellationToken)
        {
            var ws = Volatile.Read(ref _webSocket);
            if (ws?.State != WebSocketState.Open)
                throw new InvalidOperationException("WebSocket is not connected");

            var buffer = Encoding.UTF8.GetBytes(message);
            await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, cancellationToken);
        }

        private async Task RunBackgroundRecoveryLoopAsync(CancellationToken cancellationToken)
        {
            int reconnectAttempt = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Используем существующий ConnectAsync с политикой повторных попыток
                    await ConnectAsync(cancellationToken);
                    reconnectAttempt = 0; // Сброс счётчика при успешном подключении

                    // Ждём, пока соединение активно
                    while (IsConnected && !cancellationToken.IsCancellationRequested)
                    {
                        await Task.Delay(1000, cancellationToken);
                    }

                    // Если соединение разорвано, логируем и продолжаем цикл
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await LogAsync($"Соединение разорвано, начинаем переподключение...");
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    reconnectAttempt++;
                    // Экспоненциальный backoff с cap: 5, 10, 20, 40, 60, 60, 60...
                    var delay = TimeSpan.FromSeconds(
                        Math.Min(_reconnectDelay.TotalSeconds * Math.Pow(2, reconnectAttempt - 1), _maxReconnectDelay.TotalSeconds));

                    await LogAsync($"Ошибка подключения (попытка {reconnectAttempt}): {ex.Message}. Повтор через {delay.TotalSeconds}с...");

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

            await LogAsync("Фоновый цикл восстановления завершён");
        }

        protected virtual async Task ReceiveLoopAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[4096];
            var stringBuilder = new StringBuilder();
            var reconnectAttempts = 0;
            const int maxInternalReconnects = 3;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Если соединение разорвано, пытаемся переподключиться
                    if (!IsConnected)
                    {
                        if (reconnectAttempts >= maxInternalReconnects)
                        {
                            // Исчерпали внутренние попытки — выходим, внешний health-check перезапустит
                            OnErrorOccurred(new Exception(
                                $"{Name}: Max internal reconnect attempts ({maxInternalReconnects}) exhausted. " +
                                "Waiting for external restart by health-check."));
                            break;
                        }

                        await ReconnectAsync(cancellationToken);
                        reconnectAttempts++;
                        continue;
                    }

                    // Сброс счётчика после успешного подключения
                    reconnectAttempts = 0;

                    var ws = Volatile.Read(ref _webSocket);
                    if (ws == null)
                        break;

                    var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await DisconnectAsync(cancellationToken);
                        break;
                    }

                    stringBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

                    if (result.EndOfMessage)
                    {
                        var message = stringBuilder.ToString();
                        stringBuilder.Clear();
                        OnMessageReceived(message);
                        await ProcessMessageAsync(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelled
                    break;
                }
                catch (Exception ex)
                {
                    OnErrorOccurred(ex);
                    // Попытка переподключения при следующей итерации цикла
                    await Task.Delay(1000, cancellationToken); // небольшая пауза перед повторной попыткой
                }
            }
        }

        private async Task ReconnectAsync(CancellationToken cancellationToken)
        {
            // Останавливаем текущий ReceiveLoop
            StopReceiveLoop();

            // Освобождаем старое соединение, если оно есть
            var oldWs = Interlocked.Exchange(ref _webSocket, null);
            if (oldWs != null && oldWs.State != WebSocketState.Closed)
            {
                try
                {
                    await oldWs.CloseAsync(WebSocketCloseStatus.NormalClosure, "Reconnecting", cancellationToken);
                }
                catch
                {
                    // Игнорируем ошибки закрытия
                }
                oldWs.Dispose();
            }

            // Используем политику повторных попыток для подключения с созданием нового WebSocket внутри
            await _retryPolicy.ExecuteAsync(async () =>
            {
                // Создаём новый ClientWebSocket для каждой попытки
                var ws = new ClientWebSocket();
                await ws.ConnectAsync(_uri, cancellationToken);
                
                // Атомарно заменяем старый сокет (уже null), старый диспозим
                var previousWs = Interlocked.Exchange(ref _webSocket, ws);
                previousWs?.Dispose();
                
                OnConnected();
                await StartReceiveLoopAsync();
            });
        }

        /// <summary>
        /// Логирует сообщение с временной меткой.
        /// Может быть переопределён в производных классах для интеграции с системой логирования.
        /// </summary>
        protected virtual Task LogAsync(string message)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] {Name}: {message}");
            return Task.CompletedTask;
        }

        protected virtual Task ProcessMessageAsync(string message)
        {
            // Override in derived classes to process specific message formats
            return Task.CompletedTask;
        }

        protected virtual void OnMessageReceived(string message)
        {
            MessageReceived?.Invoke(this, message);
        }

        protected virtual void OnConnected()
        {
            Connected?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnDisconnected()
        {
            Disconnected?.Invoke(this, EventArgs.Empty);
        }

        protected virtual void OnErrorOccurred(Exception ex)
        {
            ErrorOccurred?.Invoke(this, ex);
        }

        public virtual void Dispose()
        {
            if (_disposed)
                return;

            // Останавливаем фоновую задачу восстановления синхронно
            try
            {
                StopAsync(CancellationToken.None).Wait(TimeSpan.FromSeconds(5));
            }
            catch
            {
                // Игнорируем ошибки при остановке фоновой задачи
            }
            
            StopReceiveLoop();
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            var ws = Interlocked.Exchange(ref _webSocket, null);
            ws?.Dispose();
            _disposed = true;
            GC.SuppressFinalize(this);
        }

        public virtual async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            // Останавливаем фоновую задачу восстановления
            await StopAsync(CancellationToken.None);
            
            StopReceiveLoop();
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            var ws = Interlocked.Exchange(ref _webSocket, null);
            if (ws != null)
            {
                if (ws.State == WebSocketState.Open)
                {
                    try
                    {
                        await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", CancellationToken.None);
                    }
                    catch
                    {
                        // Игнорируем ошибки закрытия при диспозе
                    }
                }
                ws.Dispose();
            }
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}