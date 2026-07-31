using System;

namespace MarketDataCollector.Core.Exceptions
{
    /// <summary>
    /// Нейтральное исключение персистентности. Переносит информацию об ошибке работы с хранилищем
    /// без привязки к конкретной СУБД/драйверу (Npgsql и т.п.).
    /// Application-слой обрабатывает только этот тип, не ссылаясь на технологию.
    /// </summary>
    public class PersistenceException : Exception
    {
        /// <summary>
        /// Необязательный код состояния SQL (например "23505" для unique violation).
        /// Заполняется инфраструктурным слоем, если драйвер предоставляет такую информацию.
        /// </summary>
        public string? SqlState { get; }

        public PersistenceException(string message)
            : base(message)
        {
        }

        public PersistenceException(string message, string? sqlState)
            : base(message)
        {
            SqlState = sqlState;
        }

        public PersistenceException(string message, string? sqlState, Exception innerException)
            : base(message, innerException)
        {
            SqlState = sqlState;
        }
    }
}
