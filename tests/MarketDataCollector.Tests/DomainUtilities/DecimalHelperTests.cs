using MarketDataCollector.Domain.Utilities;

namespace MarketDataCollector.Tests.DomainUtilities;

public class DecimalHelperTests
{
    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_NormalValue_TruncatedTo8Decimals()
    {
        // Arrange
        var value = 12345.67890123456m;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(12345.67890123m);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_Exactly8Decimals_Unchanged()
    {
        // Arrange
        var value = 999.12345678m;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(999.12345678m);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ValueAboveMax_ReturnsMaxValue()
    {
        // Arrange
        var value = 99999999999.99999999m; // > MaxValue (10^10)

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MaxValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ValueBelowMin_ReturnsMinValue()
    {
        // Arrange
        var value = -99999999999.99999999m; // < MinValue

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MinValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ExtremeLargeValue_DoesNotOverflow()
    {
        // Arrange
        var value = decimal.MaxValue; // ~7.9e28 — экстремально большое

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MaxValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ExtremeSmallValue_DoesNotOverflow()
    {
        // Arrange
        var value = decimal.MinValue; // ~-7.9e28 — экстремально маленькое

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MinValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_NegativeValue_TruncatedCorrectly()
    {
        // Arrange
        var value = -12345.67890123456m;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(-12345.67890123m);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_Zero_ReturnsZero()
    {
        // Arrange
        var value = 0m;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(0m);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ExactMaxValue_Unchanged()
    {
        // Arrange
        var value = DecimalHelperTests_Constants.MaxValue;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MaxValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_ExactMinValue_Unchanged()
    {
        // Arrange
        var value = DecimalHelperTests_Constants.MinValue;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(DecimalHelperTests_Constants.MinValue);
    }

    [Fact(Timeout = 5000)]
    public void TruncateForDatabase_MidRangeValue_RoundTripWorks()
    {
        // Arrange
        var value = 50000.12345678m;

        // Act
        var result = DecimalHelper.TruncateForDatabase(value);

        // Assert
        result.Should().Be(50000.12345678m);
    }
}

/// <summary>
/// Константы из DecimalHelper для использования в тестах (доступ к private не нужен).
/// </summary>
internal static class DecimalHelperTests_Constants
{
    public const decimal MaxValue = 9999999999.99999999m;
    public const decimal MinValue = -9999999999.99999999m;
}
