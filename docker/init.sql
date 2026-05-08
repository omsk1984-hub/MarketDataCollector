-- Инициализация базы данных MarketDataDb
-- Создание расширения для UUID (если нужно)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Создание таблицы для хранения сырых тиков
CREATE TABLE IF NOT EXISTS RawTicks (
    Id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    Ticker VARCHAR(20) NOT NULL,
    Price DECIMAL(18, 8) NOT NULL,
    Volume DECIMAL(18, 8) NOT NULL,
    Timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    Exchange VARCHAR(50) NOT NULL,
    ReceivedAt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    Normalized BOOLEAN DEFAULT FALSE,
    CONSTRAINT unique_tick UNIQUE (Ticker, Exchange, Timestamp)
);

-- Создание индексов для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_rawticks_ticker ON RawTicks(Ticker);
CREATE INDEX IF NOT EXISTS idx_rawticks_timestamp ON RawTicks(Timestamp);
CREATE INDEX IF NOT EXISTS idx_rawticks_exchange ON RawTicks(Exchange);

-- Создание таблицы для мониторинга подключений
CREATE TABLE IF NOT EXISTS ConnectionLogs (
    Id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    Exchange VARCHAR(50) NOT NULL,
    EventType VARCHAR(20) NOT NULL, -- 'Connected', 'Disconnected', 'Error'
    Message TEXT,
    CreatedAt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание таблицы для агрегированных данных (опционально)
CREATE TABLE IF NOT EXISTS AggregatedData (
    Id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    Ticker VARCHAR(20) NOT NULL,
    Interval VARCHAR(10) NOT NULL, -- '1min', '5min', '1hour', etc.
    OpenPrice DECIMAL(18, 8) NOT NULL,
    HighPrice DECIMAL(18, 8) NOT NULL,
    LowPrice DECIMAL(18, 8) NOT NULL,
    ClosePrice DECIMAL(18, 8) NOT NULL,
    Volume DECIMAL(18, 8) NOT NULL,
    StartTime TIMESTAMP WITH TIME ZONE NOT NULL,
    EndTime TIMESTAMP WITH TIME ZONE NOT NULL,
    CreatedAt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Создание индексов для агрегированных данных
CREATE INDEX IF NOT EXISTS idx_aggregated_ticker_interval ON AggregatedData(Ticker, Interval);
CREATE INDEX IF NOT EXISTS idx_aggregated_starttime ON AggregatedData(StartTime);

-- Комментарии к таблицам
COMMENT ON TABLE RawTicks IS 'Сырые тиковые данные с бирж';
COMMENT ON TABLE ConnectionLogs IS 'Логи подключений к источникам данных';
COMMENT ON TABLE AggregatedData IS 'Агрегированные данные по интервалам';