namespace MarketDataCollector.Core.Interfaces
{
    /// <summary>
    /// Интерфейс сервиса мониторинга состояния WebSocket-клиентов.
    /// Определён в Core, чтобы Infrastructure мог использовать без зависимости от Application.
    /// </summary>
    public interface IMonitoringService
    {
        void StartMonitoring();
        void StopMonitoring();
        void UpdateConnectionStatus(string exchange, ConnectionStatus status, string? message = null);
        void IncrementTickCounter(string exchange);
        ConnectionStatus GetConnectionStatus(string exchange);
        int GetTickCount(string exchange);
        int GetTotalTicksProcessed();
        void ResetCounters();
    }

    public enum ConnectionStatus
    {
        Connected,
        Disconnected,
        Error,
        Reconnecting
    }
}
