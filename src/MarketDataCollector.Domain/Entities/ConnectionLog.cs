using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using MarketDataCollector.Domain.Interfaces;

namespace MarketDataCollector.Domain.Entities
{
    [Table("connectionlogs")]
    public class ConnectionLog
    {
        [Key]
        [Column("id")]
        public Guid Id { get; private set; }

        [Column("exchange")]
        [Required]
        [MaxLength(50)]
        public string Exchange { get; private set; } = null!;

        [Column("eventtype")]
        [Required]
        [MaxLength(20)]
        public string EventType { get; private set; } = null!;

        [Column("message")]
        public string Message { get; private set; } = null!;

        [Column("createdat")]
        [Required]
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
