using MarketDataCollector.Core.Telemetry;
using System.Diagnostics.Metrics;

namespace MarketDataCollector.Tests.Core.Telemetry;

/// <summary>
/// Unit-тесты метрик дедупликации <see cref="MarketDataTelemetry.TicksDeduplicatedByCache"/>
/// и <see cref="MarketDataTelemetry.TicksDeduplicatedByDb"/>.
/// Проверяют, что счётчики существуют на глобальном Meter и корректно аккумулируют значения.
/// </summary>
public class DeduplicationMetricsTests
{
    private const long ExpectedCache = 5;
    private const long ExpectedDb = 3;

    private static long Observe(string instrumentName, Action emit)
    {
        long measured = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter == MarketDataTelemetry.Instance && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
        {
            if (inst.Name == instrumentName) measured += value;
        });
        listener.Start();

        emit();

        listener.Dispose();
        return measured;
    }

    [Fact]
    public void TicksDeduplicatedByCache_Exists_And_Accumulates()
    {
        // Act — эмитим значения через глобальный счётчик
        long measured = Observe(
            "ticks.deduplicated.cache",
            () =>
            {
                MarketDataTelemetry.TicksDeduplicatedByCache.Add(2, ChannelTag(0));
                MarketDataTelemetry.TicksDeduplicatedByCache.Add(3, ChannelTag(1));
            });

        // Assert
        measured.Should().Be(ExpectedCache);
    }

    [Fact]
    public void TicksDeduplicatedByDb_Exists_And_Accumulates()
    {
        // Act
        long measured = Observe(
            "ticks.deduplicated.db",
            () =>
            {
                MarketDataTelemetry.TicksDeduplicatedByDb.Add(1, ChannelTag(0));
                MarketDataTelemetry.TicksDeduplicatedByDb.Add(2, ChannelTag(1));
            });

        // Assert
        measured.Should().Be(ExpectedDb);
    }

    private static KeyValuePair<string, object?> ChannelTag(int channelIndex)
        => new("channel_index", channelIndex);
}
