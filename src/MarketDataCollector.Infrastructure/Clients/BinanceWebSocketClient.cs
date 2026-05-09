using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using System.Globalization;

namespace MarketDataCollector.Infrastructure.Clients;

/// <summary>
/// WebSocket-клиент для биржи Binance.
/// Поддерживает подписку на поток сделок (trade stream) и парсинг сообщений.
/// </summary>
public class BinanceWebSocketClient : BaseWebSocketClient
{
    private readonly Uri _webSocketUri;
    private readonly IMarketDataProcessor _dataProcessor;

    /// <summary>
    /// Создаёт экземпляр Binance WebSocket-клиента.
    /// </summary>
    public BinanceWebSocketClient(
        Uri webSocketUri,
        string exchangeName,
        string symbol,
        IMarketDataProcessor dataProcessor,
        IWebSocketConnectionManager connectionManager,
        IWebSocketMessageReceiver messageReceiver,
        IReconnectStrategy reconnectStrategy,
        ISubscriptionManager subscriptionManager,
        IOptions<WebSocketClientOptions> options,
        ILogger<BinanceWebSocketClient> logger)
        : base(webSocketUri, exchangeName, symbol, connectionManager, messageReceiver,
              reconnectStrategy, subscriptionManager, options, logger)
    {
        _webSocketUri = webSocketUri;
        _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
    }

    /// <inheritdoc />
    protected override Uri GetWebSocketUri() => _webSocketUri;

    /// <inheritdoc />
    /// <remarks>
    /// Отправляет сообщение подписки на поток сделок Binance в формате JSON.
    /// Пример: {"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":1}
    /// </remarks>
    protected override async Task SubscribeToTickerAsync(string symbol, CancellationToken cancellationToken)
    {
        var subscribeMessage = $"{{\"method\":\"SUBSCRIBE\",\"params\":[\"{symbol.ToLower()}@trade\"],\"id\":1}}";
        await SendAsync(subscribeMessage, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Парсит сообщения о сделках от Binance и передаёт данные в <see cref="IMarketDataProcessor"/>.
    /// Ожидаемый формат: {"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100",...}
    /// </remarks>
    protected override async Task ProcessMessageAsync(string message)
    {
        try
        {
            var json = JObject.Parse(message);

            if (json["e"]?.ToString() == "trade")
            {
                var ticker = json["s"]?.ToString();
                var price = decimal.Parse(json["p"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
                var volume = decimal.Parse(json["q"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
                var timeMs = long.Parse(json["T"]?.ToString() ?? "0");
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timeMs).UtcDateTime;

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
}
