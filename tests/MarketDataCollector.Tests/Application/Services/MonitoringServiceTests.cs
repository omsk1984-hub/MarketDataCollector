using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace MarketDataCollector.Tests.Application.Services;

public class MonitoringServiceTests
{
    private readonly Mock<ILogger<MonitoringService>> _loggerMock;
    private readonly MonitoringService _service;

    public MonitoringServiceTests()
    {
        _loggerMock = new Mock<ILogger<MonitoringService>>();
        _service = new MonitoringService(_loggerMock.Object);
    }

    [Fact]
    public void Constructor_CreatesService()
    {
        // Arrange & Act
        var service = new MonitoringService(_loggerMock.Object);

        // Assert
        service.Should().NotBeNull();
    }

    [Fact]
    public void StartMonitoring_StartsStatusTimer()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.StartMonitoring();

        // Assert
        // Проверяем, что логирование произошло
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Monitoring service started")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void StopMonitoring_StopsStatusTimer()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);
        service.StartMonitoring();

        // Act
        service.StopMonitoring();

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Monitoring service stopped")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void UpdateConnectionStatus_UpdatesStatusToConnected()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Connected);
    }

    [Fact]
    public void UpdateConnectionStatus_UpdatesStatusToDisconnected()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Disconnected);

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Disconnected);
    }

    [Fact]
    public void UpdateConnectionStatus_UpdatesStatusToError()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error, "Test error");

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Error);
    }

    [Fact]
    public void UpdateConnectionStatus_LogsConnectedEvent()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Binance") && o.ToString()!.Contains("Connected")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void UpdateConnectionStatus_LogsDisconnectedEvent()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Disconnected);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Binance") && o.ToString()!.Contains("Disconnected")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void UpdateConnectionStatus_LogsErrorEvent()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error, "Test error");

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Binance") && o.ToString()!.Contains("Error")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void UpdateConnectionStatus_LogsErrorMessage()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error, "Connection failed");

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Connection failed")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void IncrementTickCounter_IncrementsCounter()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.IncrementTickCounter("Binance");

        // Assert
        service.GetTickCount("Binance").Should().Be(1);
    }

    [Fact]
    public void IncrementTickCounter_IncrementsCounterMultipleTimes()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");

        // Assert
        service.GetTickCount("Binance").Should().Be(3);
    }

    [Fact]
    public void IncrementTickCounter_IncrementsTotalCount()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Kraken");

        // Assert
        service.GetTotalTicksProcessed().Should().Be(2);
    }

    [Fact]
    public void IncrementTickCounter_DifferentExchangesHaveSeparateCounters()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Kraken");

        // Assert
        service.GetTickCount("Binance").Should().Be(2);
        service.GetTickCount("Kraken").Should().Be(1);
    }

    [Fact]
    public void GetConnectionStatus_WhenNotSet_ReturnsDisconnected()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        var status = service.GetConnectionStatus("UnknownExchange");

        // Assert
        status.Should().Be(ConnectionStatus.Disconnected);
    }

    [Fact]
    public void GetTickCount_WhenNotSet_ReturnsZero()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        var count = service.GetTickCount("UnknownExchange");

        // Assert
        count.Should().Be(0);
    }

    [Fact]
    public void GetTotalTicksProcessed_ReturnsZeroInitially()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        var count = service.GetTotalTicksProcessed();

        // Assert
        count.Should().Be(0);
    }

    [Fact]
    public void ResetCounters_ResetsTickCounters()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Kraken");

        // Act
        service.ResetCounters();

        // Assert
        service.GetTickCount("Binance").Should().Be(0);
        service.GetTickCount("Kraken").Should().Be(0);
        service.GetTotalTicksProcessed().Should().Be(0);
    }

    [Fact]
    public void ResetCounters_LogsResetEvent()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.ResetCounters();

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Counters reset")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact]
    public void StartMonitoring_CanBeCalledMultipleTimes()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);

        // Act
        service.StartMonitoring();
        service.StartMonitoring();

        // Assert
        // Не должно быть исключений
        service.Should().NotBeNull();
    }

    [Fact]
    public void StopMonitoring_CanBeCalledMultipleTimes()
    {
        // Arrange
        var service = new MonitoringService(_loggerMock.Object);
        service.StartMonitoring();

        // Act
        service.StopMonitoring();
        service.StopMonitoring();

        // Assert
        // Не должно быть исключений
        service.Should().NotBeNull();
    }
}
