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
    internal readonly record struct DedupKey(string Ticker, string Exchange, long TimestampTicks)
        : IEquatable<DedupKey>
    {
        /// <summary>
        /// Комбинированный хэш через HashCode.Combine — быстрее ValueTuple.
        /// StringComparison.Ordinal для Ticker и Exchange.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            return HashCode.Combine(
                Ticker?.GetHashCode(StringComparison.Ordinal) ?? 0,
                Exchange?.GetHashCode(StringComparison.Ordinal) ?? 0,
                TimestampTicks);
        }
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

        // Счётчик добавлений — эвикция раз в N добавлений
        private int _addCount;
        private const int EvictionCheckInterval = 100;

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
        /// O(1) — Dictionary.ContainsKey.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return false;

            var key = new DedupKey(ticker, exchange, timestamp.Ticks);
            return _cache.ContainsKey(key);
        }

        /// <summary>
        /// Добавляет ключ в кэш. Если ключ уже есть — пропускает.
        /// При превышении maxSize выполняет batch-эвикцию (10% от maxSize) 
        /// раз в EvictionCheckInterval добавлений.
        /// </summary>
        public void Add(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return;

            var key = new DedupKey(ticker, exchange, timestamp.Ticks);
            // ContainsKey вместо TryAdd — совместимость с .NET Standard 2.0
            if (_cache.ContainsKey(key))
                return;

            // Эвикция: проверяем только каждый EvictionCheckInterval-й Add
            // и только если превышен лимит. За один раз удаляем _evictionBatchSize записей.
            _addCount++;
            if (_addCount % EvictionCheckInterval == 0 && _cache.Count >= _maxSize)
            {
                for (int i = 0; i < _evictionBatchSize && _order.Count > 0; i++)
                {
                    var oldest = _order.Dequeue();
                    _cache.Remove(oldest);
                }
            }

            _cache[key] = 0;
            _order.Enqueue(key);
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
            _addCount = 0;
        }
    }
}
