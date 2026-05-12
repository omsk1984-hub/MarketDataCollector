using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Entities;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace MarketDataCollector.Application.Services
{
    public class MonitoringService : Core.Interfaces.IMonitoringService
    {
        private readonly ILogger<MonitoringService> _logger;
        private readonly ITimeService _timeService;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ConcurrentDictionary<string, Core.Interfaces.ConnectionStatus> _connectionStatuses;
        private readonly ConcurrentDictionary<string, int> _tickCounters;
        private Timer _statusTimer = null!;
        private int _totalTicksProcessed;

        public MonitoringService(
            ILogger<MonitoringService> logger,
            ITimeService timeService,
            IServiceScopeFactory scopeFactory)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _timeService = timeService ?? throw new ArgumentNullException(nameof(timeService));
            _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            _connectionStatuses = new ConcurrentDictionary<string, Core.Interfaces.ConnectionStatus>();
            _tickCounters = new ConcurrentDictionary<string, int>();
            _totalTicksProcessed = 0;
        }

        public void StartMonitoring()
        {
            _statusTimer = new Timer(LogStatus, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));
            _logger.LogInformation("Monitoring service started");
        }

        public void StopMonitoring()
        {
            _statusTimer?.Dispose();
            _logger.LogInformation("Monitoring service stopped");
        }

        public void UpdateConnectionStatus(string exchange, Core.Interfaces.ConnectionStatus status, string? message = null)
        {
            _connectionStatuses[exchange] = status;

            var logMessage = $"Exchange {exchange} status changed to {status}";
            if (!string.IsNullOrEmpty(message))
                logMessage += $": {message}";

            switch (status)
            {
                case Core.Interfaces.ConnectionStatus.Connected:
                    _logger.LogInformation(logMessage);
                    break;
                case Core.Interfaces.ConnectionStatus.Disconnected:
                    _logger.LogWarning(logMessage);
                    break;
                case Core.Interfaces.ConnectionStatus.Error:
                    _logger.LogError(logMessage);
                    break;
            }

            // Сохраняем событие в БД (fire-and-forget, чтобы не блокировать event-handler)
            var eventType = status switch
            {
                Core.Interfaces.ConnectionStatus.Connected => "Connected",
                Core.Interfaces.ConnectionStatus.Disconnected => "Disconnected",
                Core.Interfaces.ConnectionStatus.Error => "Error",
                _ => status.ToString()
            };

            var dbMessage = status switch
            {
                Core.Interfaces.ConnectionStatus.Connected => $"Connected to {exchange}",
                Core.Interfaces.ConnectionStatus.Disconnected => $"Disconnected from {exchange}",
                Core.Interfaces.ConnectionStatus.Error => message ?? $"Error on {exchange}",
                _ => $"{status} on {exchange}"
            };

            _ = SaveConnectionLogAsync(exchange, eventType, dbMessage);
        }

        public void IncrementTickCounter(string exchange)
        {
            _tickCounters.AddOrUpdate(exchange, 1, (key, oldValue) => oldValue + 1);
            Interlocked.Increment(ref _totalTicksProcessed);
        }

        public Core.Interfaces.ConnectionStatus GetConnectionStatus(string exchange)
        {
            return _connectionStatuses.TryGetValue(exchange, out var status) ? status : Core.Interfaces.ConnectionStatus.Disconnected;
        }

        public int GetTickCount(string exchange)
        {
            return _tickCounters.TryGetValue(exchange, out var count) ? count : 0;
        }

        public int GetTotalTicksProcessed()
        {
            return _totalTicksProcessed;
        }

        public void ResetCounters()
        {
            _tickCounters.Clear();
            _totalTicksProcessed = 0;
            _logger.LogInformation("Counters reset");
        }

        private void LogStatus(object? state)
        {
            try
            {
                _logger.LogInformation("=== Monitoring Status ===");
                _logger.LogInformation("Total ticks processed: {TotalTicks}", _totalTicksProcessed);

                foreach (var exchange in _connectionStatuses.Keys)
                {
                    var status = _connectionStatuses[exchange];
                    var tickCount = GetTickCount(exchange);
                    _logger.LogInformation("  {Exchange}: {Status} (ticks: {TickCount})", exchange, status, tickCount);
                }

                _logger.LogInformation("=== End Status ===");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging monitoring status");
            }
        }
        private async Task SaveConnectionLogAsync(string exchange, string eventType, string message)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var repo = scope.ServiceProvider.GetService(typeof(IConnectionLogRepository)) as IConnectionLogRepository;

                if (repo == null)
                {
                    _logger.LogError("IConnectionLogRepository not registered in DI container");
                    return;
                }

                var log = new ConnectionLog(exchange, eventType, message, _timeService);
                await repo.AddAsync(log);
                await repo.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save connection log to database");
            }
        }
    }
}
