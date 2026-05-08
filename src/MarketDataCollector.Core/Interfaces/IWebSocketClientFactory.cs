using MarketDataCollector.Core.Configuration;

namespace MarketDataCollector.Core.Interfaces
{
    public interface IWebSocketClientFactory
    {
        IExchangeWebSocketClient CreateBinanceClient(string urlTemplate, string symbol, string exchangeName);
        IEnumerable<IExchangeWebSocketClient> CreateAllClients();
    }
}
