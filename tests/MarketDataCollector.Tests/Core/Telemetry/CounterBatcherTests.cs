using MarketDataCollector.Core.Telemetry;
using System.Diagnostics.Metrics;

namespace MarketDataCollector.Tests.Core.Telemetry;

/// <summary>
/// Unit-тесты <see cref="CounterBatcher"/>.
/// Проверяют: накопление без Flush (0 в OTel), вынос в Counter после Flush,
/// корректность тегов, no-op при нулевом остатке и потокобезопасность.
/// </summary>
public class CounterBatcherTests
{
    [Fact]
    public void Constructor_NullCounter_Throws()
    {
        // Arrange & Act
        Action act = () => new CounterBatcher(null!, new KeyValuePair<string, object?>[] { });

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_NullTags_Throws()
    {
        // Arrange & Act
        var meter = new Meter("test.constructor");
        var counter = meter.CreateCounter<long>("test.counter");
        Action act = () => new CounterBatcher(counter, null!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Increment_WithoutFlush_NoValuePublished_ToCounter()
    {
        // Arrange
        using var meter = new Meter("test.noflush");
        var counter = meter.CreateCounter<long>("test.noflush.counter");
        var batcher = new CounterBatcher(counter, new[] { new KeyValuePair<string, object?>("k", "v") });

        long measured = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == "test.noflush.counter") measured += value;
        });
        listener.Start();

        // Act — инкременты без Flush
        for (int i = 0; i < 5; i++) batcher.Add();

        // Assert — в OTel-счётчике ничего не появилось, локальный буфер хранит 5
        measured.Should().Be(0);
        batcher.Count.Should().Be(5);

        // Cleanup (не сбрасывать локальный буфер — просто закрыть listener)
        listener.Dispose();
    }

    [Fact]
    public void Flush_PublishesAccumulatedValue_AndResetsLocal()
    {
        // Arrange
        using var meter = new Meter("test.flush");
        var counter = meter.CreateCounter<long>("test.flush.counter");
        var batcher = new CounterBatcher(counter, new[] { new KeyValuePair<string, object?>("k", "v") });

        long measured = 0;
        var observedTags = new List<KeyValuePair<string, object?>>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            if (inst.Name == "test.flush.counter")
            {
                measured += value;
                foreach (var tag in tags)
                {
                    observedTags.Add(tag);
                }
            }
        });
        listener.Start();

        // Act
        for (int i = 0; i < 5; i++) batcher.Add();
        batcher.Flush();

        // Assert — значение вынесено в OTel, локальный буфер обнулён, теги корректны
        measured.Should().Be(5);
        batcher.Count.Should().Be(0);
        observedTags.Should().ContainSingle().Which.Key.Should().Be("k");
        observedTags.Should().ContainSingle().Which.Value.Should().Be("v");

        listener.Dispose();
    }

    [Fact]
    public void Flush_WithZeroRemainder_IsNoOp_NoAddToCounter()
    {
        // Arrange
        using var meter = new Meter("test.noop");
        var counter = meter.CreateCounter<long>("test.noop.counter");
        var batcher = new CounterBatcher(counter, new[] { new KeyValuePair<string, object?>("k", "v") });

        long measured = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == "test.noop.counter") measured += value;
        });
        listener.Start();

        // Act — Flush при нулевом остатке (без Add(0))
        batcher.Flush();
        batcher.FlushAndReset();

        // Assert — никаких измерений не было
        measured.Should().Be(0);

        listener.Dispose();
    }

    [Fact]
    public void MultipleFlush_AccumulatesTotals()
    {
        // Arrange
        using var meter = new Meter("test.multi");
        var counter = meter.CreateCounter<long>("test.multi.counter");
        var batcher = new CounterBatcher(counter, new[] { new KeyValuePair<string, object?>("k", "v") });

        long measured = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == "test.multi.counter") measured += value;
        });
        listener.Start();

        // Act — несколько циклов инкремент+flush
        for (int cycle = 0; cycle < 3; cycle++)
        {
            for (int i = 0; i < 5; i++) batcher.Add();
            batcher.Flush();
        }

        // Assert — суммарно вынесено 15
        measured.Should().Be(15);
        batcher.Count.Should().Be(0);

        listener.Dispose();
    }

    [Fact]
    public void ConcurrentAdds_TotalCount_IsCorrect_AfterFlush()
    {
        // Arrange
        using var meter = new Meter("test.concurrent");
        var counter = meter.CreateCounter<long>("test.concurrent.counter");
        var batcher = new CounterBatcher(counter, new[] { new KeyValuePair<string, object?>("k", "v") });

        long measured = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == meter) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == "test.concurrent.counter") measured += value;
        });
        listener.Start();

        const int Threads = 8;
        const int AddsPerThread = 1000;

        // Act — параллельные инкременты
        Parallel.For(0, Threads, _ =>
        {
            for (int i = 0; i < AddsPerThread; i++) batcher.Add();
        });
        batcher.Flush();

        // Assert — суммарно вынесено Threads * AddsPerThread
        measured.Should().Be(Threads * AddsPerThread);
        batcher.Count.Should().Be(0);

        listener.Dispose();
    }
}
