namespace FakeTickServer;

/// <summary>
/// Параметры командной строки для FakeTickServer.
/// Парсинг ручной — без внешних зависимостей.
/// </summary>
public class Settings
{
    /// <summary>Порт для WebSocket сервера (--port, -p). По умолчанию: 5000.</summary>
    public int Port { get; set; } = 5000;

    /// <summary>Количество тиков в секунду (--rps, -r). По умолчанию: 1000.</summary>
    public int Rps { get; set; } = 1000;

    /// <summary>Список тикеров через запятую (--symbols, -s). По умолчанию: btcusdt,ethusdt</summary>
    public string[] Symbols { get; set; } = ["btcusdt", "ethusdt"];

    /// <summary>Базовая цена в USD (--base-price, -b). По умолчанию: 50000.</summary>
    public decimal BasePrice { get; set; } = 50000m;

    /// <summary>Максимальное количество тиков (--max-ticks, -m). 0 = без лимита. По умолчанию: 0.</summary>
    public long MaxTicks { get; set; } = 0;

    /// <summary>
    /// Парсит аргументы командной строки и возвращает Settings.
    /// Поддерживаемые флаги: --port/-p, --rps/-r, --symbols/-s, --base-price/-b, --max-ticks/-m
    /// </summary>
    public static Settings Parse(string[] args)
    {
        var result = new Settings();

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--port":
                case "-p":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var port))
                        result.Port = port;
                    break;

                case "--rps":
                case "-r":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var rps))
                        result.Rps = rps;
                    break;

                case "--symbols":
                case "-s":
                    if (i + 1 < args.Length)
                    {
                        var symbolsStr = args[++i];
                        result.Symbols = symbolsStr.Split(',',
                            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                    }
                    break;

                case "--base-price":
                case "-b":
                    if (i + 1 < args.Length && decimal.TryParse(args[++i], out var basePrice))
                        result.BasePrice = basePrice;
                    break;

                case "--max-ticks":
                case "-m":
                    if (i + 1 < args.Length && long.TryParse(args[++i], out var maxTicks))
                        result.MaxTicks = maxTicks;
                    break;
            }
        }

        return result;
    }

    public override string ToString()
    {
        return $"FakeTickServer Settings: port={Port}, rps={Rps}, symbols=[{string.Join(", ", Symbols)}], basePrice={BasePrice}, maxTicks={MaxTicks}";
    }
}
