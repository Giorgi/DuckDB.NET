using System.Globalization;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// GetParts reads the fields of System.Decimal in place instead of calling decimal.GetBits, so it depends on their
// order. If a runtime ever changes it, every decimal written to DuckDB is wrong without any error: these tests
// compare GetParts with decimal.GetBits, which the runtime keeps correct.
public class DecimalPartsTests
{
    [Theory]
    [InlineData("0")]
    [InlineData("1")]
    [InlineData("-1")]
    [InlineData("1234.5678")]
    [InlineData("-1234.56")]
    // The lower 64 bits full, the upper 32 empty, and the first value that needs the upper 32.
    [InlineData("18446744073709551615")]
    [InlineData("18446744073709551616")]
    // The largest and smallest decimals: all 96 bits set.
    [InlineData("79228162514264337593543950335")]
    [InlineData("-79228162514264337593543950335")]
    // The largest scale, 28.
    [InlineData("0.0000000000000000000000000001")]
    [InlineData("-7.9228162514264337593543950335")]
    public void GetPartsMatchesGetBits(string digits)
    {
        AssertMatchesGetBits(decimal.Parse(digits, CultureInfo.InvariantCulture));
    }

    [Fact]
    public void GetPartsMatchesGetBitsForComputedValues()
    {
        // Results of arithmetic, which have scales and bit patterns that a literal does not, and a negative zero.
        AssertMatchesGetBits(1m / 3m);
        AssertMatchesGetBits(-1000m / 7m);
        AssertMatchesGetBits(new decimal(0, 0, 0, true, 5));
        AssertMatchesGetBits(new decimal(-1, -1, -1, false, 0));
        AssertMatchesGetBits(new decimal(0x12345678, unchecked((int)0x9ABCDEF0), 0x0FEDCBA9, true, 17));

        for (var i = 0; i < 1000; i++)
        {
            AssertMatchesGetBits((i - 500) * 1.000001m / (i + 1));
        }
    }

    private static void AssertMatchesGetBits(decimal value)
    {
        var bits = decimal.GetBits(value);

        value.GetParts(out var low, out var high, out var isNegative, out var scale);

        low.Should().Be((uint)bits[0] | (ulong)(uint)bits[1] << 32, "the lower 64 bits of {0}", value);
        high.Should().Be((uint)bits[2], "the upper 32 bits of {0}", value);
        isNegative.Should().Be(bits[3] < 0, "the sign of {0}", value);
        scale.Should().Be((byte)(bits[3] >> 16), "the scale of {0}", value);
    }
}
