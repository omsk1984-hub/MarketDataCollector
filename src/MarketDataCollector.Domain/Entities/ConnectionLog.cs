using System;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    public class ConnectionLog
    {
        public Guid Id { get; private set; }
        public string Exchange { get; private set; } = null!;
        public string EventType { get; private set; } = null!;
        public string Message { get; private set; } = null!;
        public DateTime CreatedAt { get; private set; }

        private ConnectionLog() { } // For EF Core

        public ConnectionLog(string exchange, string eventType, string message, ITimeService timeService)
        {
            Id = Guid.NewGuid();
            Exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));
            EventType = eventType ?? throw new ArgumentNullException(nameof(eventType));
            Message = message ?? string.Empty;
            CreatedAt = timeService?.UtcNow ?? throw new ArgumentNullException(nameof(timeService));
        }

        public static ConnectionLog CreateConnected(string exchange, ITimeService timeService, string? message = null)
        {
            return new ConnectionLog(exchange, "Connected", message ?? $"Connected to {exchange}", timeService);
        }

        public static ConnectionLog CreateDisconnected(string exchange, ITimeService timeService, string? message = null)
        {
            return new ConnectionLog(exchange, "Disconnected", message ?? $"Disconnected from {exchange}", timeService);
        }

        public static ConnectionLog CreateError(string exchange, string errorMessage, ITimeService timeService)
        {
            return new ConnectionLog(exchange, "Error", errorMessage, timeService);
        }
    }
}