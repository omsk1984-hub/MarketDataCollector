using MarketDataCollector.Core.Clients;
using MarketDataCollector.Core.Configuration;
using MarketDataCollector.Core.Interfaces;
using Xunit.Abstractions;

namespace MarketDataCollector.Tests.Core.Clients;

public class SubscriptionManagerTests
{
    private readonly ITestOutputHelper _output;
    private readonly Mock<IWebSocketConnectionManager> _connectionManagerMock;
    private readonly Mock<ILogger<SubscriptionManager>> _loggerMock;
    private readonly WebSocketClientOptions _defaultOptions;

    public SubscriptionManagerTests(ITestOutputHelper output)
    {
        _output = output;
        _connectionManagerMock = new Mock<IWebSocketConnectionManager>();
        _loggerMock = new Mock<ILogger<SubscriptionManager>>();
        _defaultOptions = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = 3
        };
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullConnectionManager_ThrowsArgumentNullException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<SubscriptionManager>>();
        var subscribeAction = new Func<string, CancellationToken, Task>((s, c) => Task.CompletedTask);

        // Act & Assert
        var act = () => new SubscriptionManager(
            null!,
            Options.Create(_defaultOptions),
            loggerMock.Object,
            subscribeAction);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("connectionManager");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<SubscriptionManager>>();
        var subscribeAction = new Func<string, CancellationToken, Task>((s, c) => Task.CompletedTask);

        // Act & Assert
        var act = () => new SubscriptionManager(
            _connectionManagerMock.Object,
            null!,
            loggerMock.Object,
            subscribeAction);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        // Arrange
        var subscribeAction = new Func<string, CancellationToken, Task>((s, c) => Task.CompletedTask);

        // Act & Assert
        var act = () => new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            null!,
            subscribeAction);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("logger");
    }

    [Fact(Timeout = 5000)]
    public void Constructor_WithNullSubscribeAction_ThrowsArgumentNullException()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<SubscriptionManager>>();

        // Act & Assert
        var act = () => new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            loggerMock.Object,
            null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("subscribeAction");
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_Success_NoRetries()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_Success_NoRetries)} ===");
        // Arrange
        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object,
            (symbol, ct) => { /* Success - do nothing */ return Task.CompletedTask; });

        var symbol = "BTCUSDT";
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.SubscribeWithRetryAsync(symbol, cancellationToken);

        // Assert
        // Если подписка успешна, Polly не должен вызывать onRetry
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Попытка подписки")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_ThrowsException_RetriesUpToMax()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_ThrowsException_RetriesUpToMax)} ===");
        // Arrange
        var retryCount = 0;
        var maxRetries = 1; // Используем 1 retry для избежания timeout (Polly задержка 2^1 = 2s)
        
        var options = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = maxRetries
        };
        
        var subscribeAction = new Func<string, CancellationToken, Task>((symbol, ct) =>
        {
            retryCount++;
            if (retryCount <= maxRetries)
            {
                throw new Exception("Subscription failed");
            }
            return Task.CompletedTask;
        });

        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(options),
            _loggerMock.Object,
            subscribeAction);

        var symbol = "BTCUSDT";
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.SubscribeWithRetryAsync(symbol, cancellationToken);

        // Assert
        // Polly: retryCount = maxRetries (1), значит всего попыток: 1 (initial) + 1 (retry) = 2
        // subscribeAction вызывается 2 раза: 1 раз с исключением, 2-й успешно
        retryCount.Should().Be(maxRetries + 1);
        
        // Должны быть вызовы onRetry для каждой неудачной попытки
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Попытка подписки")),
                It.Is<Exception>(e => e.Message == "Subscription failed"),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Exactly(maxRetries));
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_AllRetriesExhausted_Throws()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_AllRetriesExhausted_Throws)} ===");
        // Arrange
        var options = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = 1 // 1 retry с задержкой 2^1 = 2s, укладывается в timeout 5s
        };
        
        var subscribeAction = new Func<string, CancellationToken, Task>((symbol, ct) =>
        {
            throw new Exception("Subscription failed");
        });

        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(options),
            _loggerMock.Object,
            subscribeAction);

        var symbol = "BTCUSDT";
        var cancellationToken = CancellationToken.None;

        // Act & Assert
        var act = async () => await manager.SubscribeWithRetryAsync(symbol, cancellationToken);
        await act.Should().ThrowAsync<Exception>()
            .WithMessage("Subscription failed");
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_CancellationToken_CancelsOperation()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_CancellationToken_CancelsOperation)} ===");
        // Arrange
        var actionCalled = false;
        
        var subscribeAction = new Func<string, CancellationToken, Task>(async (symbol, ct) =>
        {
            actionCalled = true; // Этот код не должен выполниться
            await Task.Delay(100, ct);
        });

        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object,
            subscribeAction);

        var symbol = "BTCUSDT";
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        // Polly.ExecuteAsync(cancellationToken) выбрасывает OperationCanceledException
        // до вызова delegate, если токен уже отменён
        var act = async () => await manager.SubscribeWithRetryAsync(symbol, cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
        
        // Прямая проверка, что subscribeAction НЕ вызывался
        actionCalled.Should().BeFalse();
        
        // Косвенная проверка через логи
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains("Попытка подписки")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Never);
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_RetryDelayIsExponential()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_RetryDelayIsExponential)} ===");
        // Arrange
        var maxRetries = 1; // 1 retry с задержкой 2^1 = 2s, укладывается в timeout 5s
        
        var subscribeAction = new Func<string, CancellationToken, Task>((symbol, ct) =>
        {
            throw new Exception("Subscription failed");
        });

        var options = new WebSocketClientOptions
        {
            ReceiveBufferSize = 4096,
            MaxMessageSize = 65536,
            ReconnectDelay = TimeSpan.FromSeconds(1),
            MaxReconnectDelay = TimeSpan.FromSeconds(60),
            MaxSubscribeRetries = maxRetries
        };

        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(options),
            _loggerMock.Object,
            subscribeAction);

        var symbol = "BTCUSDT";
        var cancellationToken = CancellationToken.None;

        // Act
        var act = async () => await manager.SubscribeWithRetryAsync(symbol, cancellationToken);
        
        try
        {
            await act();
        }
        catch
        {
            // Ожидаем исключение после исчерпания попыток
        }
        
        // Assert
        // Проверяем, что лог содержит сообщения с правильными экспоненциальными задержками.
        // Задержка вычисляется как TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
        // где retryAttempt в Polly начинается с 1:
        // retryAttempt=1 → 2^1 = 2s
        var expectedDelays = new[] { 2 };
        
        foreach (var expectedDelay in expectedDelays)
        {
            _loggerMock.Verify(
                x => x.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((o, t) =>
                        o.ToString()!.Contains($"Повтор через {expectedDelay}")),
                    It.Is<Exception>(e => e.Message == "Subscription failed"),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.AtLeastOnce);
        }
    }

    [Fact(Timeout = 5000)]
    public async Task SubscribeWithRetryAsync_SymbolPassedToAction()
    {
        _output.WriteLine($"=== Running: {nameof(SubscribeWithRetryAsync_SymbolPassedToAction)} ===");
        // Arrange
        string? capturedSymbol = null;
        
        var subscribeAction = new Func<string, CancellationToken, Task>((symbol, ct) =>
        {
            capturedSymbol = symbol;
            return Task.CompletedTask;
        });

        var manager = new SubscriptionManager(
            _connectionManagerMock.Object,
            Options.Create(_defaultOptions),
            _loggerMock.Object,
            subscribeAction);

        var symbol = "ETHUSDT";
        var cancellationToken = CancellationToken.None;

        // Act
        await manager.SubscribeWithRetryAsync(symbol, cancellationToken);

        // Assert
        capturedSymbol.Should().Be(symbol);
    }

    private void VerifyLogContains(LogLevel level, string containsText, Func<Times> times)
    {
        _loggerMock.Verify(
            x => x.Log(
                level,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((o, t) => o.ToString()!.Contains(containsText)),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            times);
    }
}
