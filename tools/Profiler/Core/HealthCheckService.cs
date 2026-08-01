using System.Net.Http;
using System.Text.Json;
using MarketDataCollector.Profiler.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>Ожидание готовности Worker по health-check endpoint.</summary>
public sealed class HealthCheckService : IHealthCheckService
{
    private const int PollIntervalSec = 2;

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConsoleUI _ui;
    private readonly ILogger<HealthCheckService> _logger;

    public HealthCheckService(
        IHttpClientFactory httpClientFactory,
        IConsoleUI ui,
        ILogger<HealthCheckService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _ui = ui;
        _logger = logger;
    }

    public async Task WaitForHealthyAsync(string healthUrl, int timeoutSec, CancellationToken cancellationToken)
    {
        _ui.Info($"Ожидание готовности Worker ({healthUrl}, до {timeoutSec}с) ...");

        using HttpClient client = _httpClientFactory.CreateClient("Health");
        DateTime deadline = DateTime.UtcNow.AddSeconds(Math.Max(0, timeoutSec));

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int remaining = (int)Math.Ceiling((deadline - DateTime.UtcNow).TotalSeconds);

            try
            {
                using HttpResponseMessage response = await client.GetAsync(healthUrl, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    string body = await response.Content.ReadAsStringAsync(cancellationToken);
                    if (IsHealthyBody(body))
                    {
                        _ui.Ok("Worker готов (healthy).");
                        return;
                    }

                    _ui.Warn($"Health: статус не 'healthy' ({remaining}с осталось).");
                }
                else
                {
                    int code = (int)response.StatusCode;
                    _ui.Warn($"Health: HTTP {code} ({remaining}с осталось).");
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Health-check не удался: {Message}", ex.Message);
                _ui.Warn($"Health: недоступен ({remaining}с осталось).");
            }

            await Task.Delay(TimeSpan.FromSeconds(PollIntervalSec), cancellationToken);
        }

        _ui.Error($"Worker не стал healthy за {timeoutSec}с.");
        Environment.Exit(1);
    }

    /// <summary>Определяет статус "healthy" по JSON-ответу.</summary>
    private static bool IsHealthyBody(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return false;
        }

        try
        {
            using JsonDocument doc = JsonDocument.Parse(body);
            if (doc.RootElement.TryGetProperty("status", out JsonElement status))
            {
                return status.GetString()?.Equals("healthy", StringComparison.OrdinalIgnoreCase) == true;
            }

            // Запасной вариант: ищем "healthy" в теле.
            return body.Contains("\"healthy\"", StringComparison.OrdinalIgnoreCase);
        }
        catch (JsonException)
        {
            return body.Contains("healthy", StringComparison.OrdinalIgnoreCase);
        }
    }
}
