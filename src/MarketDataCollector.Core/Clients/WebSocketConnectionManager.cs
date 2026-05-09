using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using System.Net.WebSockets;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Управляет подключением, отключением и отправкой сообщений через ClientWebSocket.
/// Потокобезопасен: использует Interlocked для атомарной замены экземпляра сокета.
/// </summary>
public class WebSocketConnectionManager : IWebSocketConnectionManager
{
    private ClientWebSocket _webSocket = new();
    private readonly ILogger<WebSocketConnectionManager> _logger;
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    public WebSocketConnectionManager(ILogger<WebSocketConnectionManager> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public WebSocketState State => _webSocket.State;

    /// <inheritdoc />
    public bool IsConnected => _webSocket.State == WebSocketState.Open;

    /// <inheritdoc />
    public event EventHandler<WebSocketState>? StateChanged;

    /// <inheritdoc />
    public async Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        await _connectLock.WaitAsync(cancellationToken);
        try
        {
            if (IsConnected)
            {
                _logger.LogDebug("Соединение уже установлено — пропуск подключения.");
                return;
            }

            // Создаём новый ClientWebSocket для каждой попытки
            var ws = new ClientWebSocket();
            await ws.ConnectAsync(uri, cancellationToken);

            // Атомарно заменяем старый сокет, старый диспозим
            var oldWs = Interlocked.Exchange(ref _webSocket, ws);
            if (oldWs != _webSocket)
            {
                try { oldWs.Dispose(); } catch { /* игнорируем */ }
            }

            _logger.LogInformation("WebSocket подключён к {Uri}.", uri);
            OnStateChanged(ws.State);
        }
        finally
        {
            _connectLock.Release();
        }
    }

    /// <inheritdoc />
    public async Task DisconnectAsync(CancellationToken cancellationToken)
    {
        var ws = _webSocket;
        if (ws.State == WebSocketState.Open)
        {
            try
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disconnecting", cancellationToken);
                _logger.LogInformation("WebSocket отключён.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Ошибка при отключении WebSocket.");
            }
            OnStateChanged(ws.State);
        }
    }

    /// <inheritdoc />
    public async Task SendAsync(string message, CancellationToken cancellationToken)
    {
        var ws = _webSocket;
        if (!IsConnected)
            throw new InvalidOperationException("WebSocket не подключён.");

        var buffer = System.Text.Encoding.UTF8.GetBytes(message);
        await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        var ws = _webSocket;
        if (!IsConnected)
            throw new InvalidOperationException("WebSocket не подключён.");

        return await ws.ReceiveAsync(buffer, cancellationToken);
    }

    /// <summary>
    /// Освобождает текущий экземпляр ClientWebSocket.
    /// </summary>
    public void DisposeCurrentSocket()
    {
        var ws = Interlocked.Exchange(ref _webSocket, new ClientWebSocket());
        try { ws.Dispose(); } catch { /* игнорируем */ }
    }

    private void OnStateChanged(WebSocketState state)
    {
        StateChanged?.Invoke(this, state);
    }
}
