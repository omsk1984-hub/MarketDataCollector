using MarketDataCollector.Application.Services;
using MarketDataCollector.Core.Interfaces;
using MarketDataCollector.Domain.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Application.Services;

public class MonitoringServiceTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<ILogger<MonitoringService>> _loggerMock;
    private readonly Mock<ITimeService> _timeServiceMock;
    private readonly Mock<IServiceScopeFactory> _scopeFactoryMock;
    private readonly Mock<IConnectionLogRepository> _connectionLogRepoMock;
    private readonly Mock<IServiceScope> _scopeMock;
    private readonly Mock<IServiceProvider> _serviceProviderMock;

    public MonitoringServiceTests(ITestOutputHelper output)
    {
        _output = output;

        _loggerMock = new Mock<ILogger<MonitoringService>>();

        _timeServiceMock = new Mock<ITimeService>();
        _timeServiceMock.Setup(x => x.UtcNow).Returns(new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc));

        _connectionLogRepoMock = new Mock<IConnectionLogRepository>();
        _connectionLogRepoMock
            .Setup(x => x.AddAsync(It.IsAny<Domain.Entities.ConnectionLog>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _connectionLogRepoMock
            .Setup(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(1));

        _serviceProviderMock = new Mock<IServiceProvider>();
        _serviceProviderMock
            .Setup(x => x.GetService(typeof(IConnectionLogRepository)))
            .Returns(_connectionLogRepoMock.Object);

        _scopeMock = new Mock<IServiceScope>();
        _scopeMock.Setup(x => x.ServiceProvider).Returns(_serviceProviderMock.Object);

        _scopeFactoryMock = new Mock<IServiceScopeFactory>();
        _scopeFactoryMock.Setup(x => x.CreateScope()).Returns(_scopeMock.Object);
    }

    private MonitoringService CreateService()
    {
        return new MonitoringService(_loggerMock.Object, _timeServiceMock.Object, _scopeFactoryMock.Object);
    }

    [Fact(Timeout = 5000)]
    public void Constructor_CreatesService()
    {
        // Arrange & Act
        var service = CreateService();

        // Assert
        service.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new MonitoringService(null!, _timeServiceMock.Object, _scopeFactoryMock.Object);
        act.Should().Throw<ArgumentNullException>().WithParameterName("logger");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullTimeService_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new MonitoringService(_loggerMock.Object, null!, _scopeFactoryMock.Object);
        act.Should().Throw<ArgumentNullException>().WithParameterName("timeService");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullScopeFactory_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new MonitoringService(_loggerMock.Object, _timeServiceMock.Object, null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("scopeFactory");
    }

    [Fact(Timeout = 5000)]
    public void StartMonitoring_StartsStatusTimer()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.StartMonitoring();

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Monitoring service started")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public void StopMonitoring_StopsStatusTimer()
    {
        // Arrange
        var service = CreateService();
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

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_UpdatesStatusToConnected()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Connected);
    }

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_UpdatesStatusToDisconnected()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Disconnected);

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Disconnected);
    }

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_UpdatesStatusToError()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error, "Test error");

        // Assert
        service.GetConnectionStatus("Binance").Should().Be(ConnectionStatus.Error);
    }

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_LogsConnectedEvent()
    {
        // Arrange
        var service = CreateService();

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

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_LogsDisconnectedEvent()
    {
        // Arrange
        var service = CreateService();

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

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_LogsErrorEvent()
    {
        // Arrange
        var service = CreateService();

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

    [Fact(Timeout = 5000)]
    public void UpdateConnectionStatus_LogsErrorMessage()
    {
        // Arrange
        var service = CreateService();

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

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_SavesConnectionLogToDatabase()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);
        await Task.Delay(200); // Даём время fire-and-forget задаче завершиться

        // Assert
        _connectionLogRepoMock.Verify(
            x => x.AddAsync(
                It.Is<Domain.Entities.ConnectionLog>(log =>
                    log.Exchange == "Binance" &&
                    log.EventType == "Connected"),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _connectionLogRepoMock.Verify(
            x => x.SaveChangesAsync(It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_Disconnected_SavesConnectionLogToDatabase()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Disconnected);
        await Task.Delay(200); // Даём время fire-and-forget задаче завершиться

        // Assert
        _connectionLogRepoMock.Verify(
            x => x.AddAsync(
                It.Is<Domain.Entities.ConnectionLog>(log =>
                    log.Exchange == "Binance" &&
                    log.EventType == "Disconnected"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_Error_SavesConnectionLogToDatabase()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error, "Test error");
        await Task.Delay(200); // Даём время fire-and-forget задаче завершиться

        // Assert
        _connectionLogRepoMock.Verify(
            x => x.AddAsync(
                It.Is<Domain.Entities.ConnectionLog>(log =>
                    log.Exchange == "Binance" &&
                    log.EventType == "Error" &&
                    log.Message.Contains("Test error")),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_ErrorWithoutMessage_SavesDefaultErrorLog()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Error);
        await Task.Delay(200); // Даём время fire-and-forget задаче завершиться

        // Assert
        _connectionLogRepoMock.Verify(
            x => x.AddAsync(
                It.Is<Domain.Entities.ConnectionLog>(log =>
                    log.Exchange == "Binance" &&
                    log.EventType == "Error" &&
                    log.Message == "Error on Binance"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_WhenDbSaveFails_LogsError()
    {
        // Arrange
        _connectionLogRepoMock
            .Setup(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("DB error"));

        var service = CreateService();

        // Act
        service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);
        await Task.Delay(200); // Даём время fire-and-forget задаче завершиться

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Failed to save connection log")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    [Fact(Timeout = 5000)]
    public async Task UpdateConnectionStatus_WhenDbFails_DoesNotThrow()
    {
        // Arrange
        _connectionLogRepoMock
            .Setup(x => x.SaveChangesAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("DB error"));

        var service = CreateService();

        // Act & Assert — не должно быть исключения, даже при ошибке БД
        var act = () =>
        {
            service.UpdateConnectionStatus("Binance", ConnectionStatus.Connected);
            return Task.CompletedTask;
        };

        await act.Should().NotThrowAsync();
    }

    [Fact(Timeout = 5000)]
    public void IncrementTickCounter_IncrementsCounter()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.IncrementTickCounter("Binance");

        // Assert
        service.GetTickCount("Binance").Should().Be(1);
    }

    [Fact(Timeout = 5000)]
    public void IncrementTickCounter_IncrementsCounterMultipleTimes()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");

        // Assert
        service.GetTickCount("Binance").Should().Be(3);
    }

    [Fact(Timeout = 5000)]
    public void IncrementTickCounter_IncrementsTotalCount()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Kraken");

        // Assert
        service.GetTotalTicksProcessed().Should().Be(2);
    }

    [Fact(Timeout = 5000)]
    public void IncrementTickCounter_DifferentExchangesHaveSeparateCounters()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Binance");
        service.IncrementTickCounter("Kraken");

        // Assert
        service.GetTickCount("Binance").Should().Be(2);
        service.GetTickCount("Kraken").Should().Be(1);
    }

    [Fact(Timeout = 5000)]
    public void GetConnectionStatus_WhenNotSet_ReturnsDisconnected()
    {
        // Arrange
        var service = CreateService();

        // Act
        var status = service.GetConnectionStatus("UnknownExchange");

        // Assert
        status.Should().Be(ConnectionStatus.Disconnected);
    }

    [Fact(Timeout = 5000)]
    public void GetTickCount_WhenNotSet_ReturnsZero()
    {
        // Arrange
        var service = CreateService();

        // Act
        var count = service.GetTickCount("UnknownExchange");

        // Assert
        count.Should().Be(0);
    }

    [Fact(Timeout = 5000)]
    public void GetTotalTicksProcessed_ReturnsZeroInitially()
    {
        // Arrange
        var service = CreateService();

        // Act
        var count = service.GetTotalTicksProcessed();

        // Assert
        count.Should().Be(0);
    }

    [Fact(Timeout = 5000)]
    public void ResetCounters_ResetsTickCounters()
    {
        // Arrange
        var service = CreateService();
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

    [Fact(Timeout = 5000)]
    public void ResetCounters_LogsResetEvent()
    {
        // Arrange
        var service = CreateService();

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

    [Fact(Timeout = 5000)]
    public void StartMonitoring_CanBeCalledMultipleTimes()
    {
        // Arrange
        var service = CreateService();

        // Act
        service.StartMonitoring();
        service.StartMonitoring();

        // Assert
        service.Should().NotBeNull();
    }

    [Fact(Timeout = 5000)]
    public void StopMonitoring_CanBeCalledMultipleTimes()
    {
        // Arrange
        var service = CreateService();
        service.StartMonitoring();

        // Act
        service.StopMonitoring();
        service.StopMonitoring();

        // Assert
        service.Should().NotBeNull();
    }
}
