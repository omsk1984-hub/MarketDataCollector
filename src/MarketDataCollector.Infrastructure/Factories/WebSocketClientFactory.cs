using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Options;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Infrastructure.Clients;

namespace MarketDataCollector.Infrastructure.Factories
{
    public class WebSocketClientFactory : IWebSocketClientFactory
    {
        private readonly IMarketDataProcessor _dataProcessor;
        private readonly IMonitoringService _monitoringService;
        private readonly ExchangeOptions _options;

        public WebSocketClientFactory(
            IMarketDataProcessor dataProcessor,
            IMonitoringService monitoringService,
            IOptions<ExchangeOptions> options)
        {
            _dataProcessor = dataProcessor;
            _monitoringService = monitoringService;
            _options = options.Value;
        }

        public IExchangeWebSocketClient CreateBinanceClient(string urlTemplate, string symbol, string exchangeName)
        {
            var webSocketUrl = urlTemplate.Replace("{symbol}", symbol, StringComparison.OrdinalIgnoreCase);
            var client = new BinanceWebSocketClient(webSocketUrl, exchangeName, symbol, _dataProcessor);

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

        public IEnumerable<IExchangeWebSocketClient> CreateAllClients()
        {
            // Словарь для быстрого поиска URL по имени биржи
            var exchangeUrls = _options.Exchanges.ToDictionary(e => e.ExchangeName, e => e.WebSocketUrl);

            // Создание клиентов для каждого ридера
            var clients = new List<IExchangeWebSocketClient>();
            foreach (var reader in _options.Readers)
            {
                if (exchangeUrls.TryGetValue(reader.ExchangeName, out var urlTemplate))
                {
                    if (reader.ExchangeName == "binance")
                    {
                        // Подстановка символа в шаблон URL теперь внутри CreateBinanceClient
                        clients.Add(CreateBinanceClient(urlTemplate, reader.Symbol, reader.ExchangeName));
                    }
                    // Можно добавить другие биржи здесь
                }
                else
                {
                    // Логирование ошибки: биржа не найдена
                    // Пока просто пропускаем
                }
            }

            return clients;
        }
    }
}
