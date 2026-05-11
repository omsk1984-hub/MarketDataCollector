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
/// </summary>
public class WebSocketMessageReceiver : IWebSocketMessageReceiver
{
    private readonly IWebSocketConnectionManager _connectionManager;
    private readonly WebSocketClientOptions _options;
    private readonly ILogger<WebSocketMessageReceiver> _logger;

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
        _logger.LogDebug("Цикл приёма сообщений запущен.");
        
        // Используем ArrayPool для эффективного управления памятью
        var tempBuffer = ArrayPool<byte>.Shared.Rent(_options.ReceiveBufferSize);
        var messageStream = new MemoryStream(_options.ReceiveBufferSize);
        
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
                        new ArraySegment<byte>(tempBuffer), cancellationToken);

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
                                new ArraySegment<byte>(tempBuffer), cancellationToken);
                        }
                        
                        messageStream.SetLength(0); // Очищаем поток для следующего сообщения
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
                            await processMessage(message);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Ошибка при обработке сообщения.");
                            onError?.Invoke(ex);
                        }
                        finally
                        {
                            // Очищаем поток для следующего сообщения
                            messageStream.SetLength(0);
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Ожидаемое поведение при отмене
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка при приёме сообщения.");
                    onError?.Invoke(ex);
                    
                    // Небольшая пауза перед повторной попыткой
                    try
                    {
                        await Task.Delay(1000, cancellationToken);
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
            // Возвращаем буфер в пул
            ArrayPool<byte>.Shared.Return(tempBuffer);
            messageStream.Dispose();
        }

        _logger.LogDebug("Цикл приёма сообщений завершён.");
    }

    /// <inheritdoc />
    public void StopReceiveLoop()
    {
        _logger.LogDebug("Остановка цикла приёма сообщений запрошена.");
    }
}
