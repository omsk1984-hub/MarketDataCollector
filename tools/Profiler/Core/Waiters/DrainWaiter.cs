using System.Net.Http;
using System.Text.RegularExpressions;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.Waiters;

/// <summary>
/// Ожидание дренажа очередей перед вторым gcdump. Опрашивает /metrics на наличие
/// <c>processor_channel_fill_level_count{...} <число></c>; обнуление всех fill_level
/// означает завершение дренажа. При недоступности метрик — обратный отсчёт таймаута.
/// </summary>
public sealed partial class DrainWaiter : IDrainWaiter
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConsoleUI _ui;
    private readonly ILogger<DrainWaiter> _logger;
    private readonly ProfilerOptions _options;

    public DrainWaiter(
        IHttpClientFactory httpClientFactory,
        IConsoleUI ui,
        ILogger<DrainWaiter> logger,
        ProfilerOptions options)
    {
        _httpClientFactory = httpClientFactory;
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public async Task WaitForDrainAsync(int timeoutSec, string metricsUrl, CancellationToken cancellationToken)
    {
        _ui.Info($"Ожидание дренажа очередей (до {timeoutSec}с, {_options.DrainConsecutiveZeroChecks} последовательных нулевых проверок) ...");

        using HttpClient client = _httpClientFactory.CreateClient("Metrics");
        DateTime deadline = DateTime.UtcNow.AddSeconds(Math.Max(0, timeoutSec));

        int consecutiveZeroCount = 0;
        double lastTotal = 0;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int remaining = (int)Math.Ceiling((deadline - DateTime.UtcNow).TotalSeconds);
            _logger.LogDebug("Дренаж: осталось {Remaining}с, consecutiveZero={Consecutive}/{Required}",
                remaining, consecutiveZeroCount, _options.DrainConsecutiveZeroChecks);

            (bool metricsAvailable, double total) = await TryReadBacklogAsync(client, metricsUrl, cancellationToken);
            if (metricsAvailable)
            {
                lastTotal = total;
                if (total <= 0)
                {
                    consecutiveZeroCount++;
                    _logger.LogDebug("Суммарный backlog=0 ({Consecutive}/{Required})",
                        consecutiveZeroCount, _options.DrainConsecutiveZeroChecks);

                    if (consecutiveZeroCount >= _options.DrainConsecutiveZeroChecks)
                    {
                        _ui.Ok($"Очереди дренированы (backlog = 0, {consecutiveZeroCount} последовательных проверок).");
                        return;
                    }
                }
                else
                {
                    if (consecutiveZeroCount > 0)
                    {
                        _logger.LogDebug("Backlog={Total:F0} > 0 — сброс счётчика нулевых проверок (было {Count}).",
                            total, consecutiveZeroCount);
                    }
                    consecutiveZeroCount = 0;
                }
            }
            else
            {
                _ui.Warn("/metrics недоступен — жду по таймауту.");
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.DrainPollIntervalSec), cancellationToken);
        }

        _ui.Warn($"Таймаут дренажа истёк — продолжаю со вторым gcdump. " +
                 $"Последний backlog={lastTotal:F0}, consecutiveZero={consecutiveZeroCount}/{_options.DrainConsecutiveZeroChecks}.");
    }

    /// <summary>
    /// Читает /metrics и возвращает суммарный backlog fill_level по каналам.
    /// Возвращает кортеж (метрика доступна, суммарный backlog).
    /// </summary>
    private async Task<(bool MetricsAvailable, double Total)> TryReadBacklogAsync(
        HttpClient client,
        string metricsUrl,
        CancellationToken cancellationToken)
    {
        try
        {
            using HttpResponseMessage response = await client.GetAsync(metricsUrl, cancellationToken);
            response.EnsureSuccessStatusCode();

            string body = await response.Content.ReadAsStringAsync(cancellationToken);

            double total = 0;
            bool found = false;

            foreach (Match match in FillLevelRegex().Matches(body))
            {
                if (double.TryParse(match.Groups["value"].Value, out double value))
                {
                    total += value;
                    found = true;
                }
            }

            if (found)
            {
                _logger.LogDebug("Суммарный backlog: {Total:F0}", total);
                return (true, total);
            }

            // Метрика отсутствует — считаем недоступной для принятия решения.
            return (false, 0);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Не удалось прочитать /metrics: {Message}", ex.Message);
            return (false, 0);
        }
    }

    [GeneratedRegex(@"processor_channel_fill_level_count\{[^}]*\}\s+(?<value>[+\-]?[\d.]+)", RegexOptions.IgnoreCase)]
    private static partial Regex FillLevelRegex();
}
