using System.Net.WebSockets;
using FakeTickServer;

var settings = Settings.Parse(args);

Console.WriteLine(settings);
Console.WriteLine("Запуск FakeTickServer...");

var builder = WebApplication.CreateBuilder(args);

// Регистрируем Settings и Generator как singletons
builder.Services.AddSingleton(settings);
builder.Services.AddSingleton<TickGeneratorService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TickGeneratorService>());

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(30)
});

var generator = app.Services.GetRequiredService<TickGeneratorService>();

// Endpoint для WebSocket-подключений — формат как у Binance: /ws/{symbol}@trade
app.Map("/ws/{symbol}@trade", async (HttpContext context, string symbol, TickGeneratorService gen) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("Expected a WebSocket request");
        return;
    }

    var webSocket = await context.WebSockets.AcceptWebSocketAsync();

    // AddClient сам генерирует ID и возвращает его
    var clientId = gen.AddClient(webSocket, symbol);

    try
    {
        // Читаем в цикле, чтобы держать соединение открытым.
        // Сервер не ждёт сообщений от клиента, просто держит соединение.
        var buffer = new byte[1024];
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(
                new ArraySegment<byte>(buffer), CancellationToken.None);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                await webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None);
                break;
            }
        }
    }
    catch (WebSocketException)
    {
        // Клиент отключился — нормальная ситуация
    }
    catch (OperationCanceledException)
    {
        // Сервер останавливается
    }
    finally
    {
        gen.RemoveClient(clientId);
    }
});

app.MapGet("/", () => Results.Ok(new
{
    service = "FakeTickServer",
    port = settings.Port,
    rps = settings.Rps,
    symbols = settings.Symbols,
    basePrice = settings.BasePrice,
    maxTicks = settings.MaxTicks,
    clients = generator.ClientCount,
    actualRps = generator.GetCurrentRps(),
    totalTicks = generator.TotalTicksGenerated,
    isLimitReached = generator.IsLimitReached
}));

app.MapGet("/health", () => Results.Ok(new
{
    status = generator.ClientCount > 0 ? "active" : "idle",
    clients = generator.ClientCount
}));

app.Run($"http://0.0.0.0:{settings.Port}");
