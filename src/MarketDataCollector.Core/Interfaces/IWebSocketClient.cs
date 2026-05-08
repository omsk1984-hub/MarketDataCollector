using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IWebSocketClient : IDisposable
    {
        bool IsConnected { get; }
        string ExchangeName { get; }
        
        Task ConnectAsync(CancellationToken cancellationToken = default);
        Task DisconnectAsync(CancellationToken cancellationToken = default);
        Task SendAsync(string message, CancellationToken cancellationToken = default);
        
        event EventHandler<string> MessageReceived;
        event EventHandler Connected;
        event EventHandler Disconnected;
        event EventHandler<Exception> ErrorOccurred;
    }
}