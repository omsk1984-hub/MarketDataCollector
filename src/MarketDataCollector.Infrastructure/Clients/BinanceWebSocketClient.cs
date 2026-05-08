using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Interfaces;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Threading;

namespace MarketDataCollector.Infrastructure.Clients
{
    public class BinanceWebSocketClient : BaseWebSocketClient
    {
        private readonly IMarketDataProcessor _dataProcessor;

        public BinanceWebSocketClient(string webSocketUrl, string exchangeName, string symbol, IMarketDataProcessor dataProcessor)
            : base(webSocketUrl, exchangeName, symbol)
        {
            _dataProcessor = dataProcessor;
        }

        protected override async Task ProcessMessageAsync(string message)
        {
            try
            {
                var json = JObject.Parse(message);
                Console.WriteLine($">>>> [{message}]");
                // Пример формата Binance trade stream
                // {"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100","b":88,"a":50,"T":123456789,"m":true}
                
                if (json["e"]?.ToString() == "trade")
                {
                    var ticker = json["s"]?.ToString();
                    var price = decimal.Parse(json["p"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
                    var volume = decimal.Parse(json["q"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
                    var time_msk = long.Parse(json["T"]?.ToString() ?? "0");
                    var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(time_msk).UtcDateTime;
                    
                    if (ticker != null)
                    {
                        await _dataProcessor.ProcessTickAsync(ticker, price, volume, timestamp, ExchangeName);
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorOccurred(ex);
            }
        }

        public override async Task SubscribeToTicker(string symbol, CancellationToken cancellationToken)
        {
            var subscribeMessage = $"{{\"method\":\"SUBSCRIBE\",\"params\":[\"{symbol.ToLower()}@trade\"],\"id\":1}}";
            await SendAsync(subscribeMessage, cancellationToken);
        }
    }
}