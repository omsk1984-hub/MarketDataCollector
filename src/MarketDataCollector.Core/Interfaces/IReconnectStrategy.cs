using System;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Core.Interfaces;

/// <summary>
/// Стратегия переподключения WebSocket.
/// Определяет задержки между попытками и условия продолжения.
/// </summary>
public interface IReconnectStrategy
{
    /// <summary>
    /// Вычисляет задержку перед следующей попыткой переподключения.
    /// </summary>
    /// <param name="attempt">Номер текущей попытки (начиная с 1).</param>
    /// <returns>Задержка перед следующей попыткой.</returns>
    TimeSpan GetDelay(int attempt);

    /// <summary>
    /// Определяет, следует ли продолжать попытки переподключения.
    /// </summary>
    /// <param name="attempt">Номер текущей попытки.</param>
    /// <returns>true, если следует продолжить; иначе — false.</returns>
    bool ShouldRetry(int attempt);

    /// <summary>
    /// Сбрасывает внутреннее состояние стратегии (вызывается после успешного подключения).
    /// </summary>
    void Reset();
}
