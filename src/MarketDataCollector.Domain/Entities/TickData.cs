using System;

namespace MarketDataCollector.Domain.Entities
{
    /// <summary>
    /// Унифицированная структура для передачи тиковых данных между компонентами.
    /// Используется в MarketDataProcessor (основной пайплайн) и TickAggregator (агрегация свечей).
    /// readonly record struct — иммутабельна, value-type, минимальные аллокации.
    /// </summary>
    public readonly record struct TickData(
        string Ticker,
        decimal Price,
        decimal Volume,
        DateTime Timestamp,
        string Exchange
    );
}
