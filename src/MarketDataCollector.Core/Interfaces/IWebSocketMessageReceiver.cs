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
    /// <param name="processMessage">Функция обработки полного сообщения.</param>
    /// <param name="onMessageReceived">Callback при получении сообщения (для событий).</param>
    /// <param name="onError">Callback при ошибке.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task StartReceiveLoopAsync(
        Func<string, Task> processMessage,
        Action<string>? onMessageReceived,
        Action<Exception>? onError,
        CancellationToken cancellationToken);

    /// <summary>
    /// Останавливает цикл приёма сообщений и дожидается его завершения.
    /// </summary>
    /// <param name="timeout">Таймаут ожидания завершения. По умолчанию 5 секунд.</param>
    /// <returns>Task, завершающийся после остановки loop или по таймауту.</returns>
    Task StopReceiveLoopAsync(TimeSpan? timeout = null);
}
