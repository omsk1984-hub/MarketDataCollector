using System.Diagnostics;
using System.Net.Http;
using MarketDataCollector.Profiler.Options;
using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Profiler.Core;

/// <summary>
/// DelegatingHandler, логирующий метаданные HTTP-запросов (метод, URL, длительность,
/// статус, размер, категория) без логирования тела. Уровень зависит от HttpLogLevel.
/// </summary>
public sealed class HttpLoggingHandler : DelegatingHandler
{
    private readonly ILogger<HttpLoggingHandler> _logger;
    private readonly LogLevel _logLevel;

    public HttpLoggingHandler(ILogger<HttpLoggingHandler> logger, ProfilerOptions options)
    {
        _logger = logger;
        _logLevel = ParseLevel(options.HttpLogLevel);
    }

    private static LogLevel ParseLevel(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "trace" => LogLevel.Trace,
            "debug" => LogLevel.Debug,
            "information" or "info" => LogLevel.Information,
            "none" or "off" => LogLevel.None,
            _ => LogLevel.Debug,
        };
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        if (!_logger.IsEnabled(_logLevel))
        {
            return await base.SendAsync(request, cancellationToken);
        }

        string method = request.Method.ToString();
        string url = request.RequestUri?.ToString() ?? "<no-url>";
        Stopwatch sw = Stopwatch.StartNew();

        try
        {
            HttpResponseMessage response = await base.SendAsync(request, cancellationToken);
            sw.Stop();

            long? contentLength = response.Content.Headers.ContentLength;
            string size = contentLength is > 0 ? $"{contentLength:N0} bytes" : "chunked";
            int statusCode = (int)response.StatusCode;
            string category = statusCode switch
            {
                >= 200 and < 300 => "Success",
                >= 300 and < 400 => "Redirection",
                >= 400 and < 500 => "ClientError",
                >= 500 => "ServerError",
                _ => "Unknown",
            };

            Log(_logLevel,
                "HTTP {Method} {Url} -> {Status} ({Category}) {Size} in {Elapsed}ms",
                method, url, statusCode, category, size, sw.ElapsedMilliseconds);

            return response;
        }
        catch (Exception ex)
        {
            sw.Stop();
            Log(LogLevel.Error, "HTTP {Method} {Url} failed: {Message} after {Elapsed}ms",
                method, url, ex.Message, sw.ElapsedMilliseconds);
            throw;
        }
    }

    private void Log(LogLevel level, string message, params object?[] args)
    {
        if (_logger.IsEnabled(level))
        {
            _logger.Log(level, message, args);
        }
    }
}
