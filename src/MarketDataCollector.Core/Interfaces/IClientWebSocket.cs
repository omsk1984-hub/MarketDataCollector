using System.Net.WebSockets;

namespace MarketDataCollector.Core.Interfaces;

/// <summary>
/// Интерфейс для обертки над ClientWebSocket.
/// Позволяет мокировать WebSocket-соединение в тестах.
/// </summary>
public interface IClientWebSocket
{
    WebSocketState State { get; }
    
    Task ConnectAsync(Uri uri, CancellationToken cancellationToken);
    
    Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken);
    
    Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);
    
    Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusText, CancellationToken cancellationToken);
    
    void Dispose();
}
