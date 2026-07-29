-- ============================================================
-- Партиционирование таблицы RawTicks по Timestamp (по дням)
-- ============================================================
--
-- Вариант А: Ручное управление партициями (без доп. зависимостей)
-- Вариант Б: Автоматическое управление через pg_partman
--
-- ВНИМАНИЕ: Этот скрипт создаёт НОВУЮ таблицу rawticks_partitioned.
-- Для миграции с существующей таблицы rawticks нужно:
-- 1. Создать партиционированную таблицу
-- 2. Вставить данные: INSERT INTO rawticks_partitioned SELECT * FROM rawticks;
-- 3. Переименовать таблицы: ALTER TABLE rawticks RENAME TO rawticks_old; ALTER TABLE rawticks_partitioned RENAME TO rawticks;
-- ============================================================


-- ============================================================
-- ВАРИАНТ А: Ручное управление партициями
-- ============================================================

-- 1. Создание parent-таблицы с PARTITION BY RANGE
CREATE TABLE IF NOT EXISTS rawticks_partitioned (
    Id UUID NOT NULL,
    Ticker VARCHAR(20) NOT NULL,
    Price DECIMAL(18, 8) NOT NULL,
    Volume DECIMAL(18, 8) NOT NULL,
    Timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    Exchange VARCHAR(50) NOT NULL,
    ReceivedAt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    Normalized BOOLEAN DEFAULT FALSE,
    -- PRIMARY KEY должен включать partition key (Timestamp)
    PRIMARY KEY (Id, Timestamp)
) PARTITION BY RANGE (Timestamp);

-- 2. Создание дочерних партиций (по дням)
--    Создаём партиции на ±7 дней от текущей даты

-- Текущая партиция (сегодня)
CREATE TABLE IF NOT EXISTS rawticks_2026_07_29 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-29') TO ('2026-07-30');

-- Прошедшие партиции (для тестов с историческими данными)
CREATE TABLE IF NOT EXISTS rawticks_2026_07_28 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-28') TO ('2026-07-29');

CREATE TABLE IF NOT EXISTS rawticks_2026_07_27 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-27') TO ('2026-07-28');

CREATE TABLE IF NOT EXISTS rawticks_2026_07_26 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-26') TO ('2026-07-27');

CREATE TABLE IF NOT EXISTS rawticks_2026_07_25 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-25') TO ('2026-07-26');

-- Будущие партиции
CREATE TABLE IF NOT EXISTS rawticks_2026_07_30 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-30') TO ('2026-07-31');

CREATE TABLE IF NOT EXISTS rawticks_2026_07_31 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-07-31') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS rawticks_2026_08_01 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-08-01') TO ('2026-08-02');

CREATE TABLE IF NOT EXISTS rawticks_2026_08_02 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-08-02') TO ('2026-08-03');

CREATE TABLE IF NOT EXISTS rawticks_2026_08_03 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-08-03') TO ('2026-08-04');

CREATE TABLE IF NOT EXISTS rawticks_2026_08_04 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-08-04') TO ('2026-08-05');

CREATE TABLE IF NOT EXISTS rawticks_2026_08_05 PARTITION OF rawticks_partitioned
    FOR VALUES FROM ('2026-08-05') TO ('2026-08-06');

-- 3. Индексы на parent-таблице (распространяются на все партиции)
CREATE INDEX IF NOT EXISTS idx_rawticks_partitioned_ticker ON rawticks_partitioned(Ticker);
CREATE INDEX IF NOT EXISTS idx_rawticks_partitioned_exchange ON rawticks_partitioned(Exchange);
-- UNIQUE constraint по (Ticker, Exchange, Timestamp) — заменяет отдельные индексы
-- ВНИМАНИЕ: UNIQUE constraint на партиционированной таблице должен включать partition key
CREATE UNIQUE INDEX IF NOT EXISTS idx_rawticks_partitioned_unique
    ON rawticks_partitioned(Ticker, Exchange, Timestamp);

-- 4. default partition — для данных, не попадающих в существующие партиции
CREATE TABLE IF NOT EXISTS rawticks_default PARTITION OF rawticks_partitioned DEFAULT;


-- ============================================================
-- ВАРИАНТ Б: Автоматическое управление через pg_partman
-- ============================================================
-- Требует расширение pg_partman: CREATE EXTENSION pg_partman;
--
-- CREATE EXTENSION IF NOT EXISTS pg_partman;
--
-- -- Создание parent-таблицы (аналогично варианту А)
-- CREATE TABLE IF NOT EXISTS rawticks_partman (
--     Id UUID NOT NULL,
--     Ticker VARCHAR(20) NOT NULL,
--     Price DECIMAL(18, 8) NOT NULL,
--     Volume DECIMAL(18, 8) NOT NULL,
--     Timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
--     Exchange VARCHAR(50) NOT NULL,
--     ReceivedAt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
--     Normalized BOOLEAN DEFAULT FALSE,
--     PRIMARY KEY (Id, Timestamp)
-- ) PARTITION BY RANGE (Timestamp);
--
-- -- Регистрация в pg_partman
-- SELECT partman.create_parent(
--     p_parent_table := 'public.rawticks_partman',
--     p_control := 'timestamp',
--     p_type := 'native',
--     p_interval := '1 day',
--     p_premake := 7  -- создавать партиции на 7 дней вперёд
-- );
--
-- -- pg_partman автоматически создаёт партиции при вызове:
-- SELECT partman.run_maintenance();
--
-- -- Для автоматического вызова (например, через pg_cron):
-- SELECT cron.schedule('partman-maintenance', '0 0 * * *', $$SELECT partman.run_maintenance()$$);
--
-- -- Автоматическое удаление старых партиций (retention):
-- -- В настройках partman таблицы установить:
-- --   retention = '30 days' — удалять партиции старше 30 дней
-- --   retention_keep_table = false — удалять таблицу целиком


-- ============================================================
-- Миграция данных (после создания партиционированной таблицы)
-- ============================================================

-- Шаг 1: Вставка данных из существующей таблицы
-- INSERT INTO rawticks_partitioned SELECT * FROM rawticks;

-- Шаг 2: Переименование таблиц
-- ALTER TABLE rawticks RENAME TO rawticks_old;
-- ALTER TABLE rawticks_partitioned RENAME TO rawticks;

-- Шаг 3: Удаление старой таблицы (после проверки)
-- DROP TABLE rawticks_old;


-- ============================================================
-- Полезные запросы для мониторинга
-- ============================================================

-- Просмотр всех партиций
-- SELECT schemaname, tablename, partition boundaries
-- FROM pg_partitions
-- WHERE tablename = 'rawticks_partitioned';

-- Просмотр размера каждой партиции
-- SELECT
--     inhrelid::regclass AS partition,
--     pg_size_pretty(pg_total_relation_size(inhrelid)) AS size
-- FROM pg_inherits
-- WHERE inhparent = 'rawticks_partitioned'::regclass
-- ORDER BY inhrelid::regclass::text;

-- EXPLAIN ANALYZE для проверки partition pruning
-- EXPLAIN ANALYZE
-- SELECT * FROM rawticks_partitioned
-- WHERE Timestamp >= '2026-07-29' AND Timestamp < '2026-07-30'
-- AND Ticker = 'BTCUSDT';
