using System.Collections.Generic;

namespace MarketDataCollector.Core.Interfaces
{
    /// <summary>
    /// Потокобезопасный реестр реальных экземпляров WebSocket-клиентов.
    /// Worker регистрирует клиентов при старте, /health читает их состояние.
    /// Решает проблему доступа /health к живым клиентам без повторного создания
    /// (повторный вызов CreateAllClients создавал бы новые не-подключённые экземпляры).
    /// </summary>
    public interface IWebSocketClientRegistry
    {
        /// <summary>
        /// Регистрирует клиента в реестре. Идемпотентно (повторная регистрация игнорируется).
        /// </summary>
        void Register(IExchangeWebSocketClient client);

        /// <summary>
        /// Удаляет клиента из реестра.
        /// </summary>
        void Unregister(IExchangeWebSocketClient client);

        /// <summary>
        /// Очищает реестр (например, при остановке Worker).
        /// </summary>
        void Clear();

        /// <summary>
        /// Возвращает снимок зарегистрированных клиентов.
        /// </summary>
        IReadOnlyList<IExchangeWebSocketClient> GetClients();
    }
}
