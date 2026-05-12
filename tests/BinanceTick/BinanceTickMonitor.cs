using System;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
//cd tests\BinanceTick
//dotnet run
namespace BinanceTickMonitor
{
    class Program
    {
        // ==================== НАСТРОЙКИ ====================
        private const string BinanceWebSocketUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade";
        private const int MaxTicksToReceive = 10;       // Количество тиков для получения перед закрытием
        private const int ConnectionTimeoutMs = 10000;   // Таймаут подключения (мс)
        private const int ReadTimeoutMs = 5000;          // Таймаут чтения между сообщениями (мс)
        // ==================================================

        private static int _tickCount = 0;

        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine("=== Binance Tick Monitor ===");
            Console.WriteLine($"Подключение к: {BinanceWebSocketUrl}");
            Console.WriteLine($"Ожидаем {MaxTicksToReceive} тиков...\n");

            using var cts = new CancellationTokenSource();
            cts.CancelAfter(ConnectionTimeoutMs);

            try
            {
                using var ws = new ClientWebSocket();
                await ws.ConnectAsync(new Uri(BinanceWebSocketUrl), cts.Token);

                Console.WriteLine("Подключение установлено!\n");

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

                    // Пропускаем ping-сообщения
                    if (message.Contains("\"e\":\"ping\"") || message.Contains("ping"))
                    {
                        Console.WriteLine("  [Ping получен]");
                        continue;
                    }

                    _tickCount++;

                    // Парсим основные данные из JSON
                    var symbol = ExtractJsonValue(message, "s");
                    var price = ExtractJsonValue(message, "p");
                    var quantity = ExtractJsonValue(message, "q");
                    var tradeTime = ExtractJsonValue(message, "T");
                    var isBuyerMaker = ExtractJsonValue(message, "m");

                    var time = long.TryParse(tradeTime, out var t) 
                        ? DateTimeOffset.FromUnixTimeMilliseconds(t).ToLocalTime().ToString("HH:mm:ss.fff")
                        : "N/A";

                    var isBuyer = isBuyerMaker == "true";
                    Console.WriteLine($"[{_tickCount:D3}] {time} | {symbol} | Цена: {price} | Объем: {quantity} | Продавец инициатор: {(isBuyer ? "Да" : "Нет")} | Время трейда (ms): {tradeTime}");
                }
                catch (OperationCanceledException) when (readCts.IsCancellationRequested)
                {
                    Console.WriteLine("Таймаут чтения, продолжаем...");
                }
            }
        }

        private static string ExtractJsonValue(string json, string key)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (root.TryGetProperty(key, out var value))
                {
                    switch (value.ValueKind)
                    {
                        case JsonValueKind.String:
                            return value.GetString() ?? "N/A";
                        case JsonValueKind.Number:
                            return value.GetRawText();
                        case JsonValueKind.True:
                            return "true";
                        case JsonValueKind.False:
                            return "false";
                        default:
                            return value.GetRawText();
                    }
                }
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"  [Ошибка парсинга JSON для ключа '{key}']: {ex.Message}");
            }

            return "N/A";
        }
    }
}
