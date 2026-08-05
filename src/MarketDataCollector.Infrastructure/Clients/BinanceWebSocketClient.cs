using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
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
    /// <inheritdoc />
    /// <remarks>
    /// Zero-alloc парсинг через <see cref="Utf8JsonReader"/> (ref struct).
    /// Извлекает только нужные поля: e, s, p, q, T.
    /// Price/Volume парсятся из ReadOnlySpan<byte> без аллокации строки.
    /// </remarks>
    protected override Task ProcessMessageAsync(ReadOnlyMemory<byte> message)
    {
        // Не-async: тело полностью синхронно, а async-метод боксует state machine в heap-Task на каждый вызов
        // (~40-80 байт/тик при 21K сообщений/сек). ProcessTickAsync синхронен (TryWrite в Channel) и возвращает
        // завершённый Task, поэтому не await'им — данные гарантированно в Channel до возврата.
        try
        {
            // Парсинг Utf8JsonReader — ref struct, не может быть в async методе.
            // Выносим синхронный разбор в отдельный метод.
            var parsed = ParseTradeMessage(message.Span);

            if (parsed.IsTrade && parsed.Ticker != null)
            {
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(parsed.TradeTimeMs).UtcDateTime;
                // Fire-and-forget безопасен: ProcessTickAsync полностью синхронен (Interlocked + TryWrite в Channel),
                // Task уже завершён, дроп при полном канале фиксируется TryWrite синхронно.
                _dataProcessor.ProcessTickAsync(parsed.Ticker, parsed.Price, parsed.Volume, timestamp, ExchangeName);
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

        return Task.CompletedTask;
    }

    /// <summary>
    /// Результат zero-alloc парсинга trade-сообщения Binance.
    /// </summary>
    private readonly record struct TradeParseResult(
        string? Ticker,
        decimal Price,
        decimal Volume,
        long TradeTimeMs,
        bool IsTrade);

    /// <summary>
    /// Zero-alloc парсинг trade-сообщения Binance через <see cref="Utf8JsonReader"/>.
    /// Ref struct — не может быть в async, поэтому вынесен в отдельный метод.
    /// </summary>
    private static TradeParseResult ParseTradeMessage(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);

        string? ticker = null;
        decimal price = 0m;
        decimal volume = 0m;
        long timeMs = 0;
        bool isTrade = false;

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            return default;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            var prop = reader.ValueSpan;

            // Все свойства Binance trade stream — односимвольные: e,s,p,q,T,E,t,m,M
            if (prop.Length == 1)
            {
                switch ((char)prop[0])
                {
                    case 'e':
                        // eventType — строка "trade"
                        if (reader.Read() && reader.TokenType == JsonTokenType.String)
                            isTrade = reader.ValueSpan.SequenceEqual("trade"u8);
                        break;
                    case 's':
                        // symbol — строка в верхнем регистре, напр. "BTCUSDT"
                        if (reader.Read() && reader.TokenType == JsonTokenType.String)
                        {
                            // Интернирование известных символов: ноль аллокаций на тик.
                            // Строковые литералы заинтернированы компилятором, сравнение —
                            // по байтам ValueSpan (без вызова GetString/TranscodeHelper).
                            var span = reader.ValueSpan;
                            if (span.SequenceEqual("BTCUSDT"u8)) ticker = "BTCUSDT";
                            else if (span.SequenceEqual("ETHUSDT"u8)) ticker = "ETHUSDT";
                            else if (span.SequenceEqual("SOLUSDT"u8)) ticker = "SOLUSDT";
                            else ticker = reader.GetString(); // fallback для неизвестных символов
                        }
                        break;
                    case 'p':
                        // price — строка "1000.50"
                        if (reader.Read())
                            price = ParseDecimalFromUtf8(reader.ValueSpan);
                        break;
                    case 'q':
                        // quantity — строка "0.5"
                        if (reader.Read())
                            volume = ParseDecimalFromUtf8(reader.ValueSpan);
                        break;
                    case 'T':
                        // tradeTime — число (ms)
                        if (reader.Read() && reader.TokenType == JsonTokenType.Number)
                            timeMs = reader.GetInt64();
                        break;
                    default:
                        // E, t, m, M и пр. — пропускаем значение
                        if (reader.Read()) reader.Skip();
                        break;
                }
            }
            else
            {
                // Неизвестные длинные свойства — пропускаем
                reader.Skip();
            }
        }

        return new TradeParseResult(ticker, price, volume, timeMs, isTrade);
    }

    /// <summary>
    /// Парсинг decimal из UTF-8 байт без аллокации строки и без <c>decimal.TryParse</c>.
    /// Поддерживает: целые, дробные (через '.'), отрицательные.
    /// Binance price/quantity — всегда строки вида "0.001", "100", "12345.678".
    /// </summary>
    /// <remarks>
    /// Zero-copy: работает прямо по байтам, без копирования в <c>stackalloc char[]</c>
    /// и без вызова <c>decimal.TryParse</c> (который парсит символы и тратит CPU на внутренние проверки).
    /// Значение накапливается сразу в <c>decimal</c>, поэтому нет риска переполнения <c>long</c>.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static decimal ParseDecimalFromUtf8(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return 0m;

        int i = 0;
        bool neg = false;
        if (utf8[i] == (byte)'-')
        {
            neg = true;
            i++;
        }
        if (i >= utf8.Length)
            return 0m; // только знак без цифр

        decimal whole = 0m;
        bool digitsSeen = false;
        while (i < utf8.Length && utf8[i] != (byte)'.')
        {
            byte c = utf8[i];
            if (c < (byte)'0' || c > (byte)'9')
                return 0m; // некорректный символ в целой части
            whole = whole * 10m + (c - (byte)'0');
            digitsSeen = true;
            i++;
        }

        decimal frac = 0m;
        long scale = 1;
        if (i < utf8.Length) // есть '.'
        {
            i++; // skip '.'
            while (i < utf8.Length)
            {
                byte c = utf8[i];
                if (c < (byte)'0' || c > (byte)'9')
                    return 0m; // некорректный символ в дробной части
                frac = frac * 10m + (c - (byte)'0');
                scale *= 10;
                i++;
            }
        }

        // Ни одной цифры — пустое число.
        if (!digitsSeen && scale == 1)
            return 0m;

        decimal result = whole + (frac / scale);
        return neg ? -result : result;
    }
}
