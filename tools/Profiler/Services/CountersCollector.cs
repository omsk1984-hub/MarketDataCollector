using System.Diagnostics;
using System.Net.Http;
using System.Text;
using MarketDataCollector.Profiler.Core;
using MarketDataCollector.Profiler.Core.Interfaces;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Services;

/// <summary>Фоновый сбор Prometheus-метрик в CSV (UTF-8).</summary>
public sealed class CountersCollector : ICountersCollector
{
    private const string CsvHeader = "Timestamp,Metric,Labels,Type,Value,Description";
    private const int MetricsRetryCount = 6;
    private const int MetricsRetryPauseMs = 3000;

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IPrometheusParser _parser;
    private readonly IConsoleUI _ui;
    private readonly ILogger<CountersCollector> _logger;
    private readonly ProfilerOptions _options;

    public CountersCollector(
        IHttpClientFactory httpClientFactory,
        IPrometheusParser parser,
        IConsoleUI ui,
        ILogger<CountersCollector> logger,
        ProfilerOptions options)
    {
        _httpClientFactory = httpClientFactory;
        _parser = parser;
        _ui = ui;
        _logger = logger;
        _options = options;
    }

    public async Task StartAsync(string outputCsvPath, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outputCsvPath);

        using HttpClient client = _httpClientFactory.CreateClient("Metrics");

        // Проверка доступности /metrics (до 6 попыток с паузой 3с).
        if (!await EnsureMetricsAvailableAsync(client, cancellationToken))
        {
            _ui.Warn("Метрики недоступны — сбор счётчиков пропущен.");
            return;
        }

        _ui.Info($"Сбор счётчиков -> {outputCsvPath} (интервал {_options.RefreshSeconds}с) ...");

        using var writer = new StreamWriter(outputCsvPath, append: false, Encoding.UTF8);
        writer.WriteLine(CsvHeader);
        await writer.FlushAsync(cancellationToken);

        Stopwatch sw = Stopwatch.StartNew();
        int sampleNumber = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                string body = await FetchMetricsAsync(client, cancellationToken);
                IReadOnlyList<MetricSample> samples = _parser.Parse(body);

                foreach (MetricSample sample in samples)
                {
                    writer.WriteLine(CsvLine(sample));
                }

                await writer.FlushAsync(cancellationToken);
                sampleNumber++;

                _logger.LogInformation(
                    "[COUNTERS] Sample #{Sample} | {MetricCount} metrics | {Elapsed}s elapsed",
                    sampleNumber, samples.Count, (int)sw.Elapsed.TotalSeconds);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Сбой сбора счётчиков: {Message}", ex.Message);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _options.RefreshSeconds)), cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        _ui.Ok($"Сбор счётчиков завершён: {sampleNumber} образцов.");
    }

    private async Task<bool> EnsureMetricsAvailableAsync(HttpClient client, CancellationToken cancellationToken)
    {
        for (int attempt = 1; attempt <= MetricsRetryCount; attempt++)
        {
            try
            {
                using HttpResponseMessage response = await client.GetAsync(_options.MetricsUrl, cancellationToken);
                response.EnsureSuccessStatusCode();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Попытка {Attempt}: метрики недоступны ({Message}).", attempt, ex.Message);
                await Task.Delay(MetricsRetryPauseMs, cancellationToken);
            }
        }

        return false;
    }

    private async Task<string> FetchMetricsAsync(HttpClient client, CancellationToken cancellationToken)
    {
        using HttpResponseMessage response = await client.GetAsync(_options.MetricsUrl, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(cancellationToken);
    }

    /// <summary>Формирует CSV-строку образца с экранированием кавычек.</summary>
    private static string CsvLine(MetricSample sample) =>
        $"\"{Escape(sample.Timestamp)}\",\"{Escape(sample.Metric)}\",\"{Escape(sample.Labels)}\"," +
        $"\"{Escape(sample.Type)}\",\"{Escape(sample.Value)}\",\"{Escape(sample.Description)}\"";

    private static string Escape(string value) =>
        value.Replace("\"", "\"\"", StringComparison.Ordinal);
}
