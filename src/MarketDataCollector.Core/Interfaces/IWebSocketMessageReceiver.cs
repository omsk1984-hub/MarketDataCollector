using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces;

/// <summary>
/// Управляет циклом приёма и обработки сообщений WebSocket.
/// Собирает фрагментированные сообщения и вызывает обработчик.
/// </summary>
public interface IWebSocketMessageReceiver
{
    /// <summary>
    /// Запускает цикл приёма сообщений.
    /// </summary>
    /// <param name="processMessage">Функция обработки полного сообщения (байты UTF-8 без аллокации строки).</param>
    /// <param name="onMessageReceived">Callback при получении сообщения (байты UTF-8).</param>
    /// <param name="onError">Callback при ошибке.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task StartReceiveLoopAsync(
        Func<ReadOnlyMemory<byte>, Task> processMessage,
        Action<ReadOnlyMemory<byte>>? onMessageReceived,
        Action<Exception>? onError,
        CancellationToken cancellationToken);

    /// <summary>
    /// Останавливает цикл приёма сообщений и дожидается его завершения.
    /// </summary>
    /// <param name="timeout">Таймаут ожидания завершения. По умолчанию 5 секунд.</param>
    /// <returns>Task, завершающийся после остановки loop или по таймауту.</returns>
    Task StopReceiveLoopAsync(TimeSpan? timeout = null);
}
