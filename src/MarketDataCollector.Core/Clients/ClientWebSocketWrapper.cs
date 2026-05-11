using System.Net.WebSockets;
using MarketDataCollector.Core.Interfaces;

namespace MarketDataCollector.Core.Clients;

/// <summary>
/// Реализация IClientWebSocket для обертки над ClientWebSocket.
/// </summary>
public class ClientWebSocketWrapper : IClientWebSocket
{
    private readonly ClientWebSocket _inner;

    public ClientWebSocketWrapper()
    {
        _inner = new ClientWebSocket();
    }

    public ClientWebSocketWrapper(ClientWebSocket inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public WebSocketState State => _inner.State;

    public Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        return _inner.ConnectAsync(uri, cancellationToken);
    }

    public Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
    {
        return _inner.SendAsync(buffer, messageType, endOfMessage, cancellationToken);
    }

    public Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
    {
        return _inner.ReceiveAsync(buffer, cancellationToken);
    }

    public Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusText, CancellationToken cancellationToken)
    {
        return _inner.CloseAsync(closeStatus, statusText, cancellationToken);
    }

    public void Dispose()
    {
        _inner.Dispose();
    }
}
