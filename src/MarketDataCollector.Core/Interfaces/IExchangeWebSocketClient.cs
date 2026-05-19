using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    /// <summary>
    /// Интерфейс клиента WebSocket для конкретной биржи.
    /// Использует композицию вместо наследования от IWebSocketClient.
    /// </summary>
    public interface IExchangeWebSocketClient : IDisposable
    {
        /// <summary>
        /// Имя биржи (например, "binance", "kraken")
        /// </summary>
        string ExchangeName { get; }

        /// <summary>
        /// Уникальное имя клиента (может совпадать с ExchangeName или включать символ)
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Текущий символ, на который подписан клиент
        /// </summary>
        string Symbol { get; }

        /// <summary>
        /// Флаг подключения
        /// </summary>
        bool IsConnected { get; }

        /// <summary>
        /// Подключиться к WebSocket
        /// </summary>
        Task ConnectAsync(CancellationToken ct);

        /// <summary>
        /// Отключиться от WebSocket
        /// </summary>
        Task DisconnectAsync(CancellationToken ct);

        /// <summary>
        /// Запускает клиент с автоматическим восстановлением соединения.
        /// </summary>
        Task StartAsync(CancellationToken ct);

        /// <summary>
        /// Останавливает клиент и фоновое восстановление.
        /// </summary>
        Task StopAsync(CancellationToken ct);

        /// <summary>
        /// Отправить сообщение
        /// </summary>
        Task SendAsync(string message, CancellationToken ct);

        /// <summary>
        /// Подписаться на тикер
        /// </summary>
        Task SubscribeToTicker(string symbol, CancellationToken ct);

        /// <summary>
        /// Событие получения сообщения
        /// </summary>
        event EventHandler<string> MessageReceived;

        /// <summary>
        /// Событие успешного подключения
        /// </summary>
        event EventHandler Connected;

        /// <summary>
        /// Событие отключения
        /// </summary>
        event EventHandler Disconnected;

        /// <summary>
        /// Среднее количество входящих WebSocket-сообщений в секунду за последние 10 секунд.
        /// </summary>
        double GetMessagesPerSecond();

        /// <summary>
        /// Общее количество полученных WebSocket-сообщений за всё время.
        /// </summary>
        long GetTotalMessagesCount();

        /// <summary>
        /// Событие ошибки
        /// </summary>
        event EventHandler<Exception> ErrorOccurred;
    }
}
