using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces;

/// <summary>
/// Управляет подпиской на тикеры с политикой повторных попыток.
/// </summary>
public interface ISubscriptionManager
{
    /// <summary>
    /// Подписывается на тикер с автоматическими повторными попытками при ошибке.
    /// </summary>
    /// <param name="symbol">Символ для подписки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    Task SubscribeWithRetryAsync(string symbol, CancellationToken cancellationToken);
}
