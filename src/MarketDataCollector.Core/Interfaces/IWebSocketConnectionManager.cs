using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces;

/// <summary>
/// Управляет жизненным циклом WebSocket-соединения: подключение, отключение, проверка состояния.
/// </summary>
public interface IWebSocketConnectionManager
{
    /// <summary>
    /// Текущее состояние WebSocket-соединения.
    /// </summary>
    WebSocketState State { get; }

    /// <summary>
    /// Указывает, активно ли соединение (состояние Open).
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Подключается к WebSocket по указанному URI.
    /// </summary>
    /// <param name="uri">Адрес WebSocket-сервера.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task ConnectAsync(Uri uri, CancellationToken cancellationToken);

    /// <summary>
    /// Отключается от WebSocket.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task DisconnectAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Отправляет текстовое сообщение через WebSocket.
    /// </summary>
    /// <param name="message">Текст сообщения.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task SendAsync(string message, CancellationToken cancellationToken);

    /// <summary>
    /// Принимает сообщение из WebSocket.
    /// </summary>
    /// <param name="buffer">Буфер для данных.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Результат приёма.</returns>
    Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken);

    /// <summary>
    /// Событие изменения состояния соединения.
    /// </summary>
    event EventHandler<WebSocketState>? StateChanged;
}
