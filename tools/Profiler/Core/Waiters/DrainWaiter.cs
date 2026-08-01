using System.Net.Http;
using System.Text.RegularExpressions;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core.Waiters;

/// <summary>
/// Ожидание дренажа очередей перед вторым gcdump. Опрашивает /metrics на наличие
/// <c>processor_channel_fill_level_count{...} <число></c>; обнуление всех fill_level
/// означает завершение дренажа. При недоступности метрик — обратный отсчёт таймаута.
/// </summary>
public sealed partial class DrainWaiter : IDrainWaiter
{
    private const int PollIntervalSec = 2;

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConsoleUI _ui;
    private readonly ILogger<DrainWaiter> _logger;

    public DrainWaiter(
        IHttpClientFactory httpClientFactory,
        IConsoleUI ui,
        ILogger<DrainWaiter> logger)
    {
        _httpClientFactory = httpClientFactory;
        _ui = ui;
        _logger = logger;
    }

    public async Task WaitForDrainAsync(int timeoutSec, string metricsUrl, CancellationToken cancellationToken)
    {
        _ui.Info($"Ожидание дренажа очередей (до {timeoutSec}с) ...");

        using HttpClient client = _httpClientFactory.CreateClient("Metrics");
        DateTime deadline = DateTime.UtcNow.AddSeconds(Math.Max(0, timeoutSec));

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int remaining = (int)Math.Ceiling((deadline - DateTime.UtcNow).TotalSeconds);
            _logger.LogDebug("Дренаж: осталось {Remaining}с", remaining);

            (bool metricsAvailable, bool drained) = await TryReadBacklogAsync(client, metricsUrl, cancellationToken);
            if (metricsAvailable && drained)
            {
                _ui.Ok("Очереди дренированы (backlog = 0).");
                return;
            }

            if (!metricsAvailable)
            {
                _ui.Warn("/metrics недоступен — жду по таймауту.");
            }

            await Task.Delay(TimeSpan.FromSeconds(PollIntervalSec), cancellationToken);
        }

        _ui.Warn("Таймаут дренажа истёк — продолжаю со вторым gcdump.");
    }

    /// <summary>
    /// Читает /metrics и определяет, обнулён ли суммарный fill_level по каналам.
    /// Возвращает кортеж (метрика доступна, дренировано).
    /// </summary>
    private async Task<(bool MetricsAvailable, bool Drained)> TryReadBacklogAsync(
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
                return (true, total <= 0);
            }

            // Метрика отсутствует — считаем недоступной для принятия решения.
            return (false, false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug("Не удалось прочитать /metrics: {Message}", ex.Message);
            return (false, false);
        }
    }

    [GeneratedRegex(@"processor_channel_fill_level_count\{[^}]*\}\s+(?<value>[+\-]?[\d.]+)", RegexOptions.IgnoreCase)]
    private static partial Regex FillLevelRegex();
}
