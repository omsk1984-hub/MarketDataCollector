using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace MarketDataCollector.Application.Services
{
    /// <summary>
    /// Композитный ключ для кэша дедупликации.
    /// Хранит (Ticker, Exchange, TimestampTicks) как value-type с кастомным GetHashCode.
    /// Избегает аллокаций ValueTuple и boxing DateTime при хэшировании.
    /// </summary>
    /// <summary>
    /// Композитный ключ (Ticker, Exchange, TimestampTicks) как value-type с
    /// кэшированным хэшем. Хэш вычисляется ОДИН раз при создании ключа и
    /// переиспользуется при вставке в Dictionary и Queue, вместо повторного
    /// HashCode.Combine на каждый вызов GetHashCode.
    /// StringComparison.Ordinal для Ticker и Exchange.
    /// </summary>
    internal readonly struct DedupKey : IEquatable<DedupKey>
    {
        public string Ticker { get; }
        public string Exchange { get; }
        public long TimestampTicks { get; }

        private readonly int _hashCode;

        public DedupKey(string ticker, string exchange, long timestampTicks)
        {
            Ticker = ticker;
            Exchange = exchange;
            TimestampTicks = timestampTicks;
            _hashCode = HashCode.Combine(
                ticker?.GetHashCode(StringComparison.Ordinal) ?? 0,
                exchange?.GetHashCode(StringComparison.Ordinal) ?? 0,
                timestampTicks);
        }

        /// <summary>
        /// Сравнение полей — без повторного хэширования. StringComparison.Ordinal
        /// для строк (как в кэшированном хэше).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(DedupKey other) =>
            TimestampTicks == other.TimestampTicks
            && string.Equals(Ticker, other.Ticker, StringComparison.Ordinal)
            && string.Equals(Exchange, other.Exchange, StringComparison.Ordinal);

        public override bool Equals(object? obj) => obj is DedupKey other && Equals(other);

        /// <summary>
        /// Возвращает кэшированный хэш — O(1), без пересчёта HashCode.Combine.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() => _hashCode;
    }

    /// <summary>
    /// In-memory FIFO-кэш для дедупликации тиков перед записью в БД.
    /// Хранит ключи (ticker, exchange, timestamp) последних вставленных тиков.
    /// При превышении maxSize выполняется batch-эвикция (10% от maxSize) — 
    /// быстрее, чем по одному элементу за раз.
    ///
    /// Не thread-safe — каждый consumer single-threaded, блокировки не нужны.
    /// </summary>
    public class DeduplicationCache
    {
        private readonly Dictionary<DedupKey, byte> _cache;
        private readonly Queue<DedupKey> _order;
        private readonly int _maxSize;
        private readonly int _evictionBatchSize;

        /// <summary>
        /// Создаёт кэш дедупликации.
        /// </summary>
        /// <param name="maxSize">Максимальное количество записей. 0 = кэш отключён (все Contains возвращают false).</param>
        public DeduplicationCache(int maxSize = 6000)
        {
            _maxSize = maxSize;
            _cache = new Dictionary<DedupKey, byte>(maxSize > 0 ? maxSize : 0);
            _order = new Queue<DedupKey>(maxSize > 0 ? maxSize : 0);
            // Эвиктируем 10% от maxSize (но не меньше 1) за один раз
            _evictionBatchSize = Math.Max(1, maxSize / 10);
        }

        /// <summary>
        /// Проверяет, содержится ли ключ в кэше.
        /// O(1) — Dictionary.ContainsKey (кэшированный хэш DedupKey).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return false;

            return _cache.ContainsKey(new DedupKey(ticker, exchange, timestamp.Ticks));
        }

        /// <summary>
        /// Добавляет ключ в кэш. Если ключ уже есть — пропускает.
        /// При превышении maxSize выполняет batch-эвикцию (10% от maxSize).
        /// </summary>
        public void Add(string ticker, string exchange, DateTime timestamp)
        {
            TryAdd(ticker, exchange, timestamp);
        }

        /// <summary>
        /// Проверяет и добавляет ключ за один проход (один lookup Dictionary).
        /// Возвращает true, если ключ был добавлен, false — если уже существовал.
        /// Использует кэшированный хэш DedupKey — единственный HashCode на элемент батча.
        /// При превышении maxSize выполняет batch-эвикцию (10% от maxSize) ДО вставки,
        /// сохраняя FIFO-порядок (вновь добавленный ключ не эвиктируется в этом же вызове).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return false;

            var key = new DedupKey(ticker, exchange, timestamp.Ticks);

            // Проверяем уникальность (кэшированный хэш — единственный lookup).
            if (_cache.ContainsKey(key))
                return false;

            // Эвикция при переполнении выполняется ДО вставки: удаляем самые старые записи
            // (10% от maxSize, но не меньше 1), чтобы вновь добавляемый ключ гарантированно остался.
            if (_cache.Count >= _maxSize)
            {
                for (int i = 0; i < _evictionBatchSize && _order.Count > 0; i++)
                {
                    var oldest = _order.Dequeue();
                    _cache.Remove(oldest);
                }
            }

            _cache[key] = 0;
            _order.Enqueue(key);
            return true;
        }

        /// <summary>
        /// Количество записей в кэше.
        /// </summary>
        public int Count => _cache.Count;

        /// <summary>
        /// Очищает кэш.
        /// </summary>
        public void Clear()
        {
            _cache.Clear();
            _order.Clear();
        }
    }
}
