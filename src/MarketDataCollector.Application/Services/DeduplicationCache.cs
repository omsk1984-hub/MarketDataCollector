using System;
using System.Collections.Generic;

namespace MarketDataCollector.Application.Services
{
    /// <summary>
    /// In-memory FIFO-кэш для дедупликации тиков перед записью в БД.
    /// Хранит ключи (ticker, exchange, timestamp) последних вставленных тиков.
    /// При превышении maxSize самая старая запись удаляется (FIFO-эвикция).
    ///
    /// Не thread-safe — каждый consumer single-threaded, блокировки не нужны.
    /// </summary>
    public class DeduplicationCache
    {
        private readonly Dictionary<(string, string, DateTime), byte> _cache;
        private readonly Queue<(string, string, DateTime)> _order;
        private readonly int _maxSize;

        /// <summary>
        /// Создаёт кэш дедупликации.
        /// </summary>
        /// <param name="maxSize">Максимальное количество записей. 0 = кэш отключён (все Contains возвращают false).</param>
        public DeduplicationCache(int maxSize = 6000)
        {
            _maxSize = maxSize;
            _cache = new Dictionary<(string, string, DateTime), byte>(maxSize > 0 ? maxSize : 0);
            _order = new Queue<(string, string, DateTime)>(maxSize > 0 ? maxSize : 0);
        }

        /// <summary>
        /// Проверяет, содержится ли ключ в кэше.
        /// O(1) — Dictionary.ContainsKey.
        /// </summary>
        public bool Contains(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return false;

            return _cache.ContainsKey((ticker, exchange, timestamp));
        }

        /// <summary>
        /// Добавляет ключ в кэш. Если ключ уже есть — пропускает.
        /// При превышении maxSize удаляет самую старую запись (FIFO).
        /// </summary>
        public void Add(string ticker, string exchange, DateTime timestamp)
        {
            if (_maxSize <= 0)
                return;

            var key = (ticker, exchange, timestamp);
            if (_cache.ContainsKey(key))
                return;

            // Эвикция: удаляем самую старую запись, пока не освободим место
            while (_cache.Count >= _maxSize)
            {
                var oldest = _order.Dequeue();
                _cache.Remove(oldest);
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
        }
    }
}
