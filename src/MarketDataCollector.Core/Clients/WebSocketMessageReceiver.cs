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
        var buffer = new byte[_options.ReceiveBufferSize];
        var stringBuilder = new StringBuilder();

        _logger.LogDebug("Цикл приёма сообщений запущен.");

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
                    new ArraySegment<byte>(buffer), cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    _logger.LogInformation("Получен сообщение закрытия WebSocket.");
                    break;
                }

                stringBuilder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));

                if (result.EndOfMessage)
                {
                    var message = stringBuilder.ToString();
                    stringBuilder.Clear();

                    try
                    {
                        onMessageReceived?.Invoke(message);
                        await processMessage(message);
                    }
                    catch (Exception ex)
                    {
                        onError?.Invoke(ex);
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

        _logger.LogDebug("Цикл приёма сообщений завершён.");
    }

    /// <inheritdoc />
    public void StopReceiveLoop()
    {
        _logger.LogDebug("Остановка цикла приёма сообщений запрошена.");
    }
}
