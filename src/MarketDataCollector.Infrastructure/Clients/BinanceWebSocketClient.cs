using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text.Json;

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
        IOptions<WebSocketClientOptions> options,
        ILogger<BinanceWebSocketClient> logger)
        : base(webSocketUri, exchangeName, symbol, connectionManager, messageReceiver,
              reconnectStrategy, options, logger)
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
    /* Ожидаемый формат: {
                        "e": "trade",           // string: Тип события (всегда "trade")
                        "E": 1672515782136,     // int64: Время события (Event Time) в миллисекундах (UTC)
                        "s": "BNBBTC",          // string: Символ торговой пары (в верхнем регистре)
                        "t": 12345,             // int64: Уникальный идентификатор сделки (Trade ID)
                        "p": "0.001",           // string: Цена сделки
                        "q": "100",             // string: Количество (объем) в базовой валюте
                        "T": 1672515782136,     // int64: Время совершения сделки (Trade Time) в мс
                        "m": true,              // bool: Был ли покупатель мейкером? (true = покупатель был мейкером)
                        "M": true               // bool: Игнорировать (служебное поле, зарезервировано)
                    }
    */
    /// </remarks>
    protected override async Task ProcessMessageAsync(string message)
    {
        try
        {
            using var doc = JsonDocument.Parse(message);
            var root = doc.RootElement;

            if (root.TryGetProperty("e", out var eventType) && eventType.GetString() == "trade")
            {
                var ticker = root.TryGetProperty("s", out var s) ? s.GetString() : null;
                var price = root.TryGetProperty("p", out var p)
                    ? decimal.Parse(p.GetString() ?? "0", CultureInfo.InvariantCulture)
                    : 0m;
                var volume = root.TryGetProperty("q", out var q)
                    ? decimal.Parse(q.GetString() ?? "0", CultureInfo.InvariantCulture)
                    : 0m;
                var timeMs = root.TryGetProperty("T", out var T)
                    ? T.GetInt64()
                    : 0L;
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timeMs).UtcDateTime;

                if (ticker != null)
                {
                    await _dataProcessor.ProcessTickAsync(ticker, price, volume, timestamp, ExchangeName);
                }
            }
        }
        catch (JsonException ex)
        {
            OnErrorOccurred(ex);
        }
        catch (Exception ex)
        {
            OnErrorOccurred(ex);
        }
    }
}
