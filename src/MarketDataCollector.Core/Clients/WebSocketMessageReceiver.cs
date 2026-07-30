using System.Buffers;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.WebSockets;
using System.Text;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Управляет циклом приёма сообщений WebSocket.
/// Собирает фрагментированные сообщения и вызывает обработчик при получении полного сообщения.
/// Потокобезопасен: поддерживает корректную остановку текущего loop перед запуском нового.
/// </summary>
public class WebSocketMessageReceiver : IWebSocketMessageReceiver
{
    private readonly IWebSocketConnectionManager _connectionManager;
    private readonly WebSocketClientOptions _options;
    private readonly ILogger<WebSocketMessageReceiver> _logger;

    private readonly object _loopLock = new();
    private Task? _currentLoopTask;
    private CancellationTokenSource? _loopCts;
    private static readonly TimeSpan DefaultStopTimeout = TimeSpan.FromSeconds(5);

    public WebSocketMessageReceiver(
        IWebSocketConnectionManager connectionManager,
        IOptions<WebSocketClientOptions> options,
        ILogger<WebSocketMessageReceiver> logger)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task StartReceiveLoopAsync(
        Func<string, Task> processMessage,
        Action<string>? onMessageReceived,
        Action<Exception>? onError,
        CancellationToken cancellationToken)
    {
        // Останавливаем предыдущий loop и дожидаемся его завершения.
        // Это гарантирует, что только один receive loop работает в любой момент времени.
        await StopReceiveLoopAsync().ConfigureAwait(false);

        _logger.LogDebug("Цикл приёма сообщений запущен.");

        // Создаём linked CTS: внешняя отмена + наш внутренний CTS для остановки
        lock (_loopLock)
        {
            _loopCts?.Dispose();
            _loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }
        var linkedToken = _loopCts.Token;

        // Запускаем loop и сохраняем ссылку на Task
        var task = RunLoopCoreAsync(processMessage, onMessageReceived, onError, linkedToken);

        lock (_loopLock)
        {
            _currentLoopTask = task;
        }

        // Не await'им — loop работает в фоне. Caller (BaseWebSocketClient) наблюдаемый Task.
        await Task.CompletedTask;
    }

    /// <summary>
    /// Основной цикл приёма сообщений.
    /// </summary>
    private async Task RunLoopCoreAsync(
        Func<string, Task> processMessage,
        Action<string>? onMessageReceived,
        Action<Exception>? onError,
        CancellationToken cancellationToken)
    {
        // Используем ArrayPool для эффективного управления памятью
        var tempBuffer = ArrayPool<byte>.Shared.Rent(_options.ReceiveBufferSize);
        var messageStream = new MemoryStream(_options.MaxMessageSize);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (!_connectionManager.IsConnected)
                    {
                        _logger.LogWarning("Соединение разорвано. Ожидание переподключения...");
                        break;
                    }

                    var result = await _connectionManager.ReceiveAsync(
                        new ArraySegment<byte>(tempBuffer), cancellationToken).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("Получено сообщение закрытия WebSocket.");
                        break;
                    }

                    // Проверяем, не превышает ли фрагмент максимальный размер сообщения
                    if (messageStream.Length + result.Count > _options.MaxMessageSize)
                    {
                        _logger.LogWarning(
                            "Сообщение превышает максимальный размер ({0} байт). Отбрасываем сообщение.",
                            _options.MaxMessageSize);

                        // Пропускаем оставшиеся фрагменты до EndOfMessage
                        while (!result.EndOfMessage && !cancellationToken.IsCancellationRequested)
                        {
                            result = await _connectionManager.ReceiveAsync(
                                new ArraySegment<byte>(tempBuffer), cancellationToken).ConfigureAwait(false);
                        }

                        messageStream.SetLength(0);
                        continue;
                    }

                    // Записываем фрагмент в поток
                    messageStream.Write(tempBuffer, 0, result.Count);

                    if (result.EndOfMessage)
                    {
                        try
                        {
                            // Декодируем сообщение из потока
                            var message = Encoding.UTF8.GetString(messageStream.GetBuffer(), 0, (int)messageStream.Length);

                            onMessageReceived?.Invoke(message);
                            await processMessage(message).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Ошибка при обработке сообщения.");
                            onError?.Invoke(ex);
                        }
                        finally
                        {
                            messageStream.SetLength(0);
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка при приёме сообщения.");
                    onError?.Invoke(ex);

                    // Небольшая пауза перед повторной попыткой
                    try
                    {
                        await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(tempBuffer);
            messageStream.Dispose();
            _logger.LogDebug("Цикл приёма сообщений завершён.");
        }
    }

    /// <inheritdoc />
    public async Task StopReceiveLoopAsync(TimeSpan? timeout = null)
    {
        Task? taskToWait;
        CancellationTokenSource? ctsToCancel;

        lock (_loopLock)
        {
            taskToWait = _currentLoopTask;
            ctsToCancel = _loopCts;
        }

        // Если loop не запущен или уже завершился — выходим сразу
        if (taskToWait == null || taskToWait.IsCompleted)
        {
            lock (_loopLock)
            {
                _currentLoopTask = null;
                _loopCts?.Dispose();
                _loopCts = null;
            }
            return;
        }

        // Отменяем CTS receive loop
        try
        {
            ctsToCancel?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // CTS уже disposed — игнорируем
        }

        // Дожидаемся завершения loop с таймаутом
        var effectiveTimeout = timeout ?? DefaultStopTimeout;
        try
        {
            await Task.WhenAny(taskToWait, Task.Delay(effectiveTimeout)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ошибка при остановке цикла приёма сообщений.");
        }

        // Чистим состояние
        lock (_loopLock)
        {
            _currentLoopTask = null;
            _loopCts?.Dispose();
            _loopCts = null;
        }
    }
}
