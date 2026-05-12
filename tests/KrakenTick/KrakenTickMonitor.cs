using System;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
//cd tests\KrakenTick
//dotnet run
namespace KrakenTickMonitor
{
    class Program
    {
        // ==================== НАСТРОЙКИ ====================
        private const string KrakenWebSocketUrl = "wss://ws.kraken.com/v2";
        private const string SubscribeMessage = "{\"method\": \"subscribe\", \"params\": {\"channel\": \"trade\", \"symbol\": [\"BTC/USD\"]}}";
        private const int MaxTicksToReceive = 10;       // Количество тиков для получения перед закрытием
        private const int ConnectionTimeoutMs = 10000;   // Таймаут подключения (мс)
        private const int ReadTimeoutMs = 5000;          // Таймаут чтения между сообщениями (мс)
        // ==================================================

        private static int _tickCount = 0;

        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine("=== Kraken Tick Monitor ===");
            Console.WriteLine($"Подключение к: {KrakenWebSocketUrl}");
            Console.WriteLine($"Ожидаем {MaxTicksToReceive} тиков...\n");

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(ConnectionTimeoutMs);

            try
            {
                using var ws = new ClientWebSocket();
                await ws.ConnectAsync(new Uri(KrakenWebSocketUrl), cts.Token);

                Console.WriteLine("Подключение установлено!");

                // Отправляем подписку на канал trade
                var subscribeBytes = Encoding.UTF8.GetBytes(SubscribeMessage);
                await ws.SendAsync(subscribeBytes, WebSocketMessageType.Text, true, cts.Token);
                Console.WriteLine($"Отправлена подписка: {SubscribeMessage}\n");

                await ReceiveTicks(ws, cts.Token);

                Console.WriteLine($"\nПолучено {MaxTicksToReceive} тиков. Закрытие соединения...");
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Done", CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("\nТаймаут подключения истек.");
            }
            catch (WebSocketException ex)
            {
                Console.WriteLine($"\nОшибка WebSocket: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nОшибка: {ex.Message}");
            }

            Console.WriteLine("Приложение завершено.");
        }

        private static async Task ReceiveTicks(ClientWebSocket ws, CancellationToken ct)
        {
            var buffer = new byte[4096];

            while (_tickCount < MaxTicksToReceive && !ct.IsCancellationRequested)
            {
                using var readCts = new CancellationTokenSource(ReadTimeoutMs);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, readCts.Token);

                try
                {
                    var result = await ws.ReceiveAsync(buffer, linkedCts.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        Console.WriteLine("Сервер закрыл соединение.");
                        break;
                    }

                    var message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    // Пропускаем heartbeat-сообщения
                    if (message.Contains("\"event\":\"heartbeat\"") || message.Contains("heartbeat"))
                    {
                        Console.WriteLine("  [Heartbeat]");
                        continue;
                    }

                    // Проверяем, является ли сообщение trade-данными
                    if (IsTradeMessage(message))
                    {
                        _tickCount++;

                        // Парсим данные из массива data
                        var (symbol, price, quantity, side, tradeId, timestamp) = ParseFirstTickData(message);

                        Console.WriteLine($"[{_tickCount:D3}] {timestamp} | {symbol} | Цена: {price} | Объем: {quantity} | Сторона: {side} | ID: {tradeId}");
                    }
                    else
                    {
                        // Выводим системные сообщения для отладки
                        Console.WriteLine($"  [Системное сообщение] {message}");
                    }
                }
                catch (OperationCanceledException) when (readCts.IsCancellationRequested)
                {
                    Console.WriteLine("Таймаут чтения, продолжаем...");
                }
            }
        }

        private static bool IsTradeMessage(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Проверяем channel == "trade"
                if (!root.TryGetProperty("channel", out var channel) || channel.GetString() != "trade")
                    return false;

                // Проверяем наличие data с типом array
                if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
                    return false;

                return data.GetArrayLength() > 0;
            }
            catch
            {
                return false;
            }
        }

        private static (string Symbol, string Price, string Quantity, string Side, string TradeId, string Timestamp) ParseFirstTickData(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Получаем массив data
                if (root.TryGetProperty("data", out var dataArray) && dataArray.ValueKind == JsonValueKind.Array && dataArray.GetArrayLength() > 0)
                {
                    var firstTick = dataArray[0];

                    var symbol = GetStringOrRaw(firstTick, "symbol");
                    var price = GetStringOrRaw(firstTick, "price");
                    var quantity = GetStringOrRaw(firstTick, "qty");
                    var side = GetStringOrRaw(firstTick, "side");
                    var tradeId = GetStringOrRaw(firstTick, "trade_id");
                    var timestamp = GetStringOrRaw(firstTick, "timestamp");

                    return (symbol, price, quantity, side, tradeId, timestamp);
                }
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"  [Ошибка парсинга JSON]: {ex.Message}");
            }

            return ("N/A", "N/A", "N/A", "N/A", "N/A", "N/A");
        }

        private static string GetStringOrRaw(JsonElement element, string key)
        {
            if (element.TryGetProperty(key, out var value))
            {
                return value.ValueKind switch
                {
                    JsonValueKind.String => value.GetString() ?? "N/A",
                    JsonValueKind.Number => value.GetRawText(),
                    JsonValueKind.True => "true",
                    JsonValueKind.False => "false",
                    _ => value.GetRawText()
                };
            }
            return "N/A";
        }
    }
}
