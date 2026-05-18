using System;

namespace MarketDataCollector.Domain.Utilities
{
    /// <summary>
    /// Хелпер для обрезки decimal-значений под DECIMAL(18,8) в PostgreSQL.
    /// DECIMAL(18,8) = до 10 цифр целой части + 8 цифр дробной части.
    /// Макс. значение: 9999999999.99999999 (~10 млрд).
    /// </summary>
    public static class DecimalHelper
    {
        private const decimal MaxValue = 9999999999.99999999m;
        private const decimal MinValue = -9999999999.99999999m;
        private static readonly decimal FractionMask = 1_0000_0000m; // 10^8

        /// <summary>
        /// Обрезает decimal до формата (18,8): максимум 10 цифр до запятой, 8 после.
        /// </summary>
        public static decimal TruncateForDatabase(decimal value)
        {
            // 1. Обрезаем дробную часть до 8 знаков (без округления)
            value = Math.Truncate(value * FractionMask) / FractionMask;

            // 2. Обрезаем по максимальному/минимальному допустимому значению
            if (value > MaxValue) return MaxValue;
            if (value < MinValue) return MinValue;

            return value;
        }
    }
}
