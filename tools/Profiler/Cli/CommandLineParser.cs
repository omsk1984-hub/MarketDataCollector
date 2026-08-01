using MarketDataCollector.Profiler.Options;

namespace MarketDataCollector.Profiler.Cli;

/// <summary>
/// Парсер аргументов командной строки. Поддерживает форматы
/// <c>--name value</c> и <c>--name=value</c>, регистронезависимо
/// (camelCase и kebab-case).
/// </summary>
public static class CommandLineParser
{
    /// <summary>Допустимые значения профиля trace.</summary>
    private static readonly HashSet<string> AllowedTraceProfiles = new(StringComparer.OrdinalIgnoreCase)
    {
        "gc-verbose",
        "cpu-sampling",
        "contention",
        "contention-cpu",
    };

    /// <summary>
    /// Разбирает аргументы в <see cref="ProfilerOptions"/>.
    /// При неизвестном/некорректном аргументе печатает ошибку и завершает процесс кодом 1.
    /// </summary>
    public static ProfilerOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        ProfilerOptions options = new();

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];

            if (arg is "--help" or "-h")
            {
                PrintHelp();
                Environment.Exit(0);
            }

            if (!arg.StartsWith("--", StringComparison.Ordinal))
            {
                PrintError($"Неизвестный аргумент: {arg}");
                Environment.Exit(1);
            }

            // Отбрасываем "--" и разбиваем name=value.
            string body = arg[2..];
            string name;
            string? inlineValue = null;

            int eqIndex = body.IndexOf('=');
            if (eqIndex >= 0)
            {
                name = body[..eqIndex];
                inlineValue = body[(eqIndex + 1)..];
            }
            else
            {
                name = body;
            }

            name = Normalize(name);
            string? value = inlineValue;

            // Если значение не задано через '=', берём следующий аргумент.
            if (value is null)
            {
                if (i + 1 >= args.Length)
                {
                    PrintError($"Аргумент --{name} требует значение.");
                    Environment.Exit(1);
                }

                value = args[i + 1];
                i++;
            }

            Apply(options, name, value);
        }

        return options;
    }

    /// <summary>
    /// Приводит имя аргумента к единому виду: удаляет дефисы и приводит к нижнему регистру,
    /// чтобы <c>--trace-duration</c> и <c>--TraceDuration</c> были эквивалентны.
    /// </summary>
    private static string Normalize(string name) =>
        name.Replace("-", string.Empty, StringComparison.Ordinal).ToLowerInvariant();

    /// <summary>Применяет разобранное значение к опциям.</summary>
    private static void Apply(ProfilerOptions options, string name, string value)
    {
        switch (name)
        {
            case "traceprofile":
                if (!AllowedTraceProfiles.Contains(value))
                {
                    PrintError(
                        $"Недопустимое значение --trace-profile: \"{value}\". " +
                        $"Допустимые: {string.Join(", ", AllowedTraceProfiles)}.");
                    Environment.Exit(1);
                }

                options = options with { TraceProfile = value.ToLowerInvariant() };
                break;

            case "traceduration":
                options = options with { TraceDuration = ParseInt(name, value) };
                break;

            case "gcdumpatpeaksec":
                options = options with { GcDumpAtPeakSec = ParseInt(name, value) };
                break;

            case "drainwaitsec":
                options = options with { DrainWaitSec = ParseInt(name, value) };
                break;

            case "workerprocessname":
                options = options with { WorkerProcessName = value };
                break;

            case "metricsurl":
                options = options with { MetricsUrl = value };
                break;

            case "healthurl":
                options = options with { HealthUrl = value };
                break;

            case "healthtimeoutsec":
                options = options with { HealthTimeoutSec = ParseInt(name, value) };
                break;

            case "outputdir":
                options = options with { OutputDir = value };
                break;

            case "refreshseconds":
                options = options with { RefreshSeconds = ParseInt(name, value) };
                break;

            case "httploglevel":
                options = options with { HttpLogLevel = value };
                break;

            case "httpport":
                options = options with { HttpPort = ParseInt(name, value) };
                break;

            case "httpenabled":
                options = options with { HttpEnabled = ParseBool(name, value) };
                break;

            default:
                PrintError($"Неизвестный аргумент: --{name}");
                Environment.Exit(1);
                break;
        }
    }

    private static int ParseInt(string name, string value)
    {
        if (!int.TryParse(value, out int result))
        {
            PrintError($"Аргумент --{name} ожидает целое число, получено: \"{value}\".");
            Environment.Exit(1);
        }

        return result;
    }

    private static bool ParseBool(string name, string value)
    {
        if (bool.TryParse(value, out bool result))
        {
            return result;
        }

        PrintError($"Аргумент --{name} ожидает true/false, получено: \"{value}\".");
        Environment.Exit(1);
        return false;
    }

    private static void PrintError(string message)
    {
        Console.Error.WriteLine($"ОШИБКА: {message}");
        Console.Error.WriteLine("Используйте --help для справки.");
    }

    private static void PrintHelp()
    {
        Console.WriteLine("""
            Profiler — утилита профилирования MarketDataCollector.Worker.

            Использование:
              Profiler [опции]

            Опции:
              --trace-profile <name>        Профиль dotnet-trace (gc-verbose|cpu-sampling|contention|contention-cpu)
              --trace-duration <sec>        Длительность trace, сек (по умолчанию 90)
              --gc-dump-at-peak-sec <sec>   Момент первого gcdump, сек (по умолчанию 50)
              --drain-wait-sec <sec>        Ожидание дренажа перед вторым gcdump, сек (по умолчанию 30)
              --worker-process-name <name>  Имя процесса Worker (по умолчанию MarketDataCollector.Worker)
              --metrics-url <url>           Prometheus metrics endpoint (по умолчанию http://localhost:5010/metrics)
              --health-url <url>            Health-check endpoint (по умолчанию http://localhost:5010/health)
              --health-timeout-sec <sec>    Таймаут ожидания healthy, сек (по умолчанию 30)
              --output-dir <path>           Директория результатов (по умолчанию ./traces)
              --refresh-seconds <sec>       Интервал опроса метрик, сек (по умолчанию 5)
              --http-log-level <level>      Уровень логирования HTTP (Trace|Debug|Information|None)
              --http-port <port>            Порт встроенного health-сервера профайлера (по умолчанию 5100)
              --http-enabled <bool>         Включить встроенный health-сервер профайлера (по умолчанию true)
              --help, -h                    Показать справку
            """);
    }
}
