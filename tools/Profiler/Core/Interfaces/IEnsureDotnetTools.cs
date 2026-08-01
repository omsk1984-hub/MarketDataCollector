namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Обеспечивает наличие глобальных dotnet-инструментов (dotnet-trace, dotnet-gcdump).</summary>
public interface IEnsureDotnetTools
{
    /// <summary>Проверяет установку и при необходимости доустанавливает инструменты.</summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task EnsureAsync(CancellationToken cancellationToken);
}
