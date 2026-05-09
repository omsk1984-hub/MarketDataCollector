using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;

namespace MarketDataCollector.Infrastructure.Factories;

/// <summary>
/// Фабрика для создания WebSocket-клиентов с полным набором зависимостей.
/// Использует двухфазную инициализацию для разрешения циклической зависимости
/// между клиентом и SubscriptionManager.
/// </summary>
public class WebSocketClientFactory : IWebSocketClientFactory
{
    private readonly IMarketDataProcessor _dataProcessor;
    private readonly IMonitoringService _monitoringService;
    private readonly ExchangeOptions _exchangeOptions;
    private readonly WebSocketClientOptions _wsOptions;
    private readonly ILoggerFactory _loggerFactory;

    public WebSocketClientFactory(
        IMarketDataProcessor dataProcessor,
        IMonitoringService monitoringService,
        IOptions<ExchangeOptions> exchangeOptions,
        IOptions<WebSocketClientOptions> wsOptions,
        ILoggerFactory loggerFactory)
    {
        _dataProcessor = dataProcessor ?? throw new ArgumentNullException(nameof(dataProcessor));
        _monitoringService = monitoringService ?? throw new ArgumentNullException(nameof(monitoringService));
        _exchangeOptions = exchangeOptions?.Value ?? throw new ArgumentNullException(nameof(exchangeOptions));
        _wsOptions = wsOptions?.Value ?? throw new ArgumentNullException(nameof(wsOptions));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    /// <inheritdoc />
    public IExchangeWebSocketClient CreateBinanceClient(string urlTemplate, string symbol, string exchangeName)
    {
        var webSocketUri = new Uri(urlTemplate.Replace("{symbol}", symbol, StringComparison.OrdinalIgnoreCase));
        var wsOptionsWrapper = Options.Create(_wsOptions);

        var connectionLogger = _loggerFactory.CreateLogger<WebSocketConnectionManager>();
        var receiverLogger = _loggerFactory.CreateLogger<WebSocketMessageReceiver>();
        var reconnectLogger = _loggerFactory.CreateLogger<ExponentialReconnectStrategy>();
        var subscriptionLogger = _loggerFactory.CreateLogger<SubscriptionManager>();
        var clientLogger = _loggerFactory.CreateLogger<BinanceWebSocketClient>();

        var connectionManager = new WebSocketConnectionManager(connectionLogger);
        var messageReceiver = new WebSocketMessageReceiver(connectionManager, wsOptionsWrapper, receiverLogger);
        var reconnectStrategy = new ExponentialReconnectStrategy(wsOptionsWrapper, reconnectLogger);

        // Фаза 1: Создаём клиент без SubscriptionManager
        var client = new BinanceWebSocketClient(
            webSocketUri,
            exchangeName,
            symbol,
            _dataProcessor,
            connectionManager,
            messageReceiver,
            reconnectStrategy,
            wsOptionsWrapper,
            clientLogger);

        // Фаза 2: Создаём SubscriptionManager с делегатом, который вызывает публичный SubscribeToTicker клиента
        var subscriptionManager = new SubscriptionManager(
            connectionManager,
            wsOptionsWrapper,
            subscriptionLogger,
            async (sym, ct) => await client.SubscribeToTicker(sym, ct));

        // Устанавливаем SubscriptionManager в клиент
        client.SetSubscriptionManager(subscriptionManager);

        // Интеграция с мониторингом через события клиента
        client.Connected += (sender, args) =>
        {
            _monitoringService.UpdateConnectionStatus(exchangeName, ConnectionStatus.Connected);
        };
        client.Disconnected += (sender, args) =>
        {
            _monitoringService.UpdateConnectionStatus(exchangeName, ConnectionStatus.Disconnected);
        };
        client.ErrorOccurred += (sender, ex) =>
        {
            _monitoringService.UpdateConnectionStatus(exchangeName, ConnectionStatus.Error, ex.Message);
        };
        client.MessageReceived += (sender, message) =>
        {
            _monitoringService.IncrementTickCounter(exchangeName);
        };

        return client;
    }

    /// <inheritdoc />
    public IEnumerable<IExchangeWebSocketClient> CreateAllClients()
    {
        var exchangeUrls = _exchangeOptions.Exchanges.ToDictionary(e => e.ExchangeName, e => e.WebSocketUrl);

        var clients = new List<IExchangeWebSocketClient>();
        foreach (var reader in _exchangeOptions.Readers)
        {
            if (exchangeUrls.TryGetValue(reader.ExchangeName, out var urlTemplate))
            {
                if (reader.ExchangeName == "binance")
                {
                    clients.Add(CreateBinanceClient(urlTemplate, reader.Symbol, reader.ExchangeName));
                }
                // Можно добавить другие биржи здесь
            }
        }

        return clients;
    }
}
