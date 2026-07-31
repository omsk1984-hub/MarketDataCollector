using System;
using System.Collections.Generic;
using MarketDataCollector.Core.Interfaces;

namespace MarketDataCollector.Core.Clients
{
    /// <summary>
    /// Потокобезопасная реализация <see cref="IWebSocketClientRegistry"/>.
    /// Хранит реальные экземпляры WebSocket-клиентов, созданные Worker'ом,
    /// чтобы /health мог читать их живое состояние.
    /// </summary>
    public sealed class WebSocketClientRegistry : IWebSocketClientRegistry
    {
        private readonly object _lock = new();
        private readonly List<IExchangeWebSocketClient> _clients = new();

        public void Register(IExchangeWebSocketClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            lock (_lock)
            {
                if (!_clients.Contains(client))
                    _clients.Add(client);
            }
        }

        public void Unregister(IExchangeWebSocketClient client)
        {
            lock (_lock)
            {
                _clients.Remove(client);
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _clients.Clear();
            }
        }

        public IReadOnlyList<IExchangeWebSocketClient> GetClients()
        {
            lock (_lock)
            {
                return _clients.ToArray();
            }
        }
    }
}
