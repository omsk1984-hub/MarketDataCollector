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
    public abstract class BaseWebSocketClient : IExchangeWebSocketClient
    {
        private ClientWebSocket _webSocket;
        private readonly Uri _uri;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private bool _disposed;
        private readonly AsyncRetryPolicy _retryPolicy;
        private Task _receiveLoopTask;
        private CancellationTokenSource _receiveLoopCts;

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
            });
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

            StopReceiveLoop();
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            var ws = Interlocked.Exchange(ref _webSocket, null);
            ws?.Dispose();
            _disposed = true;
            GC.SuppressFinalize(this);
        }
    }
}