using System;
using System.Net.WebSockets;
using System.Text;
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

                    // Пропускаем системные сообщения (heartbeat, status, subscriptionStatus)
                    if (message.Contains("\"channel\":\"trade\"") && message.Contains("\"data\""))
                    {
                        _tickCount++;

                        // Парсим данные из массива data
                        var symbol = ExtractJsonValue(message, "symbol");
                        var price = ExtractJsonValue(message, "price");
                        var quantity = ExtractJsonValue(message, "qty");
                        var side = ExtractJsonValue(message, "side");
                        var tradeId = ExtractJsonValue(message, "trade_id");
                        var timestamp = ExtractJsonValue(message, "timestamp");

                        Console.WriteLine($"[{_tickCount:D3}] {timestamp} | {symbol} | Цена: {price} | Объем: {quantity} | Сторона: {side} | ID: {tradeId}");
                    }
                    else
                    {
                        // Выводим системные сообщения для отладки
                        var channel = ExtractJsonValue(message, "channel");
                        if (!string.IsNullOrEmpty(channel) && channel != "N/A")
                        {
                            Console.WriteLine($"  [Системное] channel: {channel}");
                        }
                    }
                }
                catch (OperationCanceledException) when (readCts.IsCancellationRequested)
                {
                    Console.WriteLine("Таймаут чтения, продолжаем...");
                }
            }
        }

        private static string ExtractJsonValue(string json, string key)
        {
            var search = $"\"{key}\":\"";
            var start = json.IndexOf(search);
            if (start < 0)
            {
                // Пробуем найти числовое значение
                search = $"\"{key}\":";
                start = json.IndexOf(search);
                if (start < 0) return "N/A";
                start += search.Length;
                var end = json.IndexOfAny(new[] { ',', '}', ']' }, start);
                if (end < 0) return "N/A";
                return json[start..end].Trim();
            }

            start += search.Length;
            var endQuote = json.IndexOf("\"", start);
            if (endQuote < 0) return "N/A";

            return json[start..endQuote];
        }
    }
}
