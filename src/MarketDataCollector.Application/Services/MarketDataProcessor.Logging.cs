using Microsoft.Extensions.Logging;

namespace MarketDataCollector.Application.Services;

/// <summary>
/// Source-generated LoggerMessage methods for hot-path logging.
/// Eliminates string interpolation allocations in MarketDataProcessor.
/// </summary>
public partial class MarketDataProcessor
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information,
        Message = "Всего: {TotalInserted} вставлено, {TotalReceived} получено (batch={BatchSize}, filtered={Filtered}, cached={Cached}, вставлено={Inserted}")]
    partial void LogPeriodicProgress(int totalInserted, int totalReceived, int batchSize, int filtered, int cached, int inserted);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning,
        Message = "Session={SessionId}: Превышен таймаут ожидания дочитывания backlog (30с). Остаток в каналах будет потерян.")]
    partial void LogShutdownTimeout(Guid sessionId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information,
        Message = "Session={SessionId}: Остановка обработчика. Остаток в каналах: {Remaining}, всего входящих: {Incoming}, получено из канала: {Received}, вставлено: {Inserted}")]
    partial void LogStopStatistics(Guid sessionId, int remaining, int incoming, int received, int inserted);

    [LoggerMessage(EventId = 4, Level = LogLevel.Information,
        Message = "Session={SessionId}: Обработчик рыночных данных остановлен. Входящих: {Incoming}, получено из канала: {Received}, вставлено в БД: {Inserted}, реально дропнуто: {DroppedReal}, backlog (incoming-received): {DroppedCalc}, остаток в каналах: {Remaining}")]
    partial void LogFinalStopStatistics(Guid sessionId, int incoming, int received, int inserted, int droppedReal, int droppedCalc, int remaining);

    [LoggerMessage(EventId = 5, Level = LogLevel.Information,
        Message = "Session={SessionId}: Обработчик рыночных данных запущен: Single Consumer mode, batchSize={BatchSize}, ChannelCapacity={Capacity}")]
    partial void LogSingleConsumerStart(Guid sessionId, int batchSize, int capacity);

    [LoggerMessage(EventId = 6, Level = LogLevel.Information,
        Message = "Session={SessionId}: Обработчик рыночных данных запущен: {ConsumerCount} consumer'ов ({CountSource}), batchSize={BatchSize}, ChannelCapacity={Capacity}, routing=roundRobin")]
    partial void LogMultiConsumerStart(Guid sessionId, int consumerCount, string countSource, int batchSize, int capacity);

    [LoggerMessage(EventId = 7, Level = LogLevel.Error,
        Message = "Предыдущая задача обработки завершилась ошибкой, перезапуск")]
    partial void LogPreviousTaskFailed();

    [LoggerMessage(EventId = 8, Level = LogLevel.Warning,
        Message = "Session={SessionId}: Старый канал[{Index}] содержит {Count} необработанных тиков перед заменой. Это указывает на ошибку порядка запуска — клиенты писали данные до старта процессора.")]
    partial void LogOldChannelHasData(Guid sessionId, int index, int count);

    [LoggerMessage(EventId = 9, Level = LogLevel.Critical,
        Message = "Session={SessionId}: Неожиданная ошибка в consumer channel={Channel}")]
    partial void LogConsumerCriticalError(Exception ex, Guid sessionId, int channel);

    [LoggerMessage(EventId = 10, Level = LogLevel.Debug,
        Message = "Session={SessionId}: Таймерный сброс частичного батча: {Count} тиков (batchSize={BatchSize}), channel={Channel}")]
    partial void LogTimerFlush(Guid sessionId, int count, int batchSize, int channel);

    [LoggerMessage(EventId = 11, Level = LogLevel.Debug,
        Message = "Session={SessionId}: Финальный сброс channel={Channel}: {Count} тиков (batchSize={BatchSize})")]
    partial void LogFinalFlush(Guid sessionId, int channel, int count, int batchSize);

    [LoggerMessage(EventId = 12, Level = LogLevel.Warning,
        Message = "Обработка батча отменена")]
    partial void LogBatchCancelled();

    [LoggerMessage(EventId = 13, Level = LogLevel.Information,
        Message = "Session={SessionId}: channel={Channel} обработка отменена")]
    partial void LogChannelCancelled(Guid sessionId, int channel);

    [LoggerMessage(EventId = 14, Level = LogLevel.Error,
        Message = "Persistence error SqlState={SqlState} writing batch {Count} ticks (channel={Channel})")]
    partial void LogPersistenceError(Exception ex, string sqlState, int count, int channel);

    [LoggerMessage(EventId = 16, Level = LogLevel.Error,
        Message = "Неожиданная ошибка при обработке батча из {Count} тиков (channel={Channel})")]
    partial void LogUnexpectedBatchError(Exception ex, int count, int channel);

    [LoggerMessage(EventId = 17, Level = LogLevel.Debug,
        Message = "Тик добавлен в очередь: {Ticker} {Price} {Volume} {Exchange}")]
    partial void LogProcessTickDebug(string ticker, decimal price, decimal volume, string exchange);

    [LoggerMessage(EventId = 18, Level = LogLevel.Debug,
        Message = "Session={SessionId}: Flush timer skip — partial batch too small: {Count} < {MinSize} ticks (channel={Channel})")]
    partial void LogTimerFlushSkipped(Guid sessionId, int count, int minSize, int channel);
}
