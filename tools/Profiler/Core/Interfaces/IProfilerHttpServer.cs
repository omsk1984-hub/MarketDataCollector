namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>
/// Встроенный HTTP-сервер профайлера (Kestrel), отдающий собственный `/health`
/// с JSON-статусом и live-метриками для ручного просмотра.
/// </summary>
public interface IProfilerHttpServer
{
    /// <summary>Запускает сервер (асинхронно) на настроенном порту.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Плавно останавливает сервер.</summary>
    Task StopAsync(CancellationToken cancellationToken);
}
