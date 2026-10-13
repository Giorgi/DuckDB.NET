using System.Globalization;
using System.Reflection;
using System.Runtime.Loader;

namespace DuckDB.NET.Test;

public class DuckDBHugeIntTests
{
    private static readonly BigInteger TwoToThe64 = BigInteger.Pow(2, 64);

    // The limits are set once, when the type is first used, and a failure there leaves the type unusable for the
    // rest of the process. So the test loads a second copy of the assembly and reads the limits from that copy,
    // under a culture that does not write the negative sign as "-".
    [Fact]
    public void LimitsDoNotDependOnTheCurrentCulture()
    {
        var culture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        culture.NumberFormat.NegativeSign = "~";

        var previous = CultureInfo.CurrentCulture;
        var context = new AssemblyLoadContext(nameof(LimitsDoNotDependOnTheCurrentCulture), isCollectible: true);

        try
        {
            CultureInfo.CurrentCulture = culture;

            var assembly = context.LoadFromAssemblyPath(typeof(DuckDBHugeInt).Assembly.Location);

            Limit(assembly, typeof(DuckDBHugeInt), nameof(DuckDBHugeInt.HugeIntMinValue)).Should().Be(-BigInteger.Pow(2, 127));
            Limit(assembly, typeof(DuckDBHugeInt), nameof(DuckDBHugeInt.HugeIntMaxValue)).Should().Be(BigInteger.Pow(2, 127) - 1);
            Limit(assembly, typeof(DuckDBUHugeInt), nameof(DuckDBUHugeInt.HugeIntMinValue)).Should().Be(BigInteger.Zero);
            Limit(assembly, typeof(DuckDBUHugeInt), nameof(DuckDBUHugeInt.HugeIntMaxValue)).Should().Be(BigInteger.Pow(2, 128) - 1);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
            context.Unload();
        }
    }

    // The constructor and ToBigInteger both go through Int128, so a round trip alone would not notice a mistake
    // they share. The two halves are compared with the ones that division gives: the value is
    // Upper * 2^64 + Lower, with Lower never negative.
    [Theory]
    [InlineData("0")]
    [InlineData("1")]
    [InlineData("-1")]
    [InlineData("9223372036854775807")]                         // the largest long
    [InlineData("9223372036854775808")]
    [InlineData("-9223372036854775808")]                        // the smallest long
    [InlineData("-9223372036854775809")]
    [InlineData("18446744073709551615")]                        // 2^64 - 1: the lower half is all ones
    [InlineData("18446744073709551616")]                        // 2^64: the lower half is zero
    [InlineData("-18446744073709551616")]
    [InlineData("-18446744073709551617")]
    [InlineData("170141183460469231731687303715884105727")]     // the largest HUGEINT
    [InlineData("-170141183460469231731687303715884105728")]    // the smallest HUGEINT
    public void HugeIntToBigIntegerGivesBackTheValue(string text)
    {
        var value = BigInteger.Parse(text, CultureInfo.InvariantCulture);

        var hugeInt = new DuckDBHugeInt(value);

        var upper = BigInteger.DivRem(value, TwoToThe64, out var lower);

        if (lower < 0)
        {
            lower += TwoToThe64;
            upper -= 1;
        }

        ((BigInteger)hugeInt.Upper).Should().Be(upper);
        ((BigInteger)hugeInt.Lower).Should().Be(lower);
        hugeInt.ToBigInteger().Should().Be(value);
        hugeInt.ToInt128().Should().Be(Int128.Parse(text, CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("1")]
    [InlineData("9223372036854775808")]
    [InlineData("18446744073709551615")]
    [InlineData("18446744073709551616")]
    [InlineData("170141183460469231731687303715884105728")]     // above the largest HUGEINT: the top bit is set
    [InlineData("340282366920938463463374607431768211455")]     // the largest UHUGEINT
    public void UHugeIntToBigIntegerGivesBackTheValue(string text)
    {
        var value = BigInteger.Parse(text, CultureInfo.InvariantCulture);

        var hugeInt = new DuckDBUHugeInt(value);

        var upper = BigInteger.DivRem(value, TwoToThe64, out var lower);

        ((BigInteger)hugeInt.Upper).Should().Be(upper);
        ((BigInteger)hugeInt.Lower).Should().Be(lower);
        hugeInt.ToBigInteger().Should().Be(value);
        hugeInt.ToUInt128().Should().Be(UInt128.Parse(text, CultureInfo.InvariantCulture));
    }

    // The conversion to Int128 or UInt128 throws OverflowException by itself. Callers get the exception and the
    // message they got before that conversion was used.
    [Theory]
    [InlineData("170141183460469231731687303715884105728")]     // the largest HUGEINT + 1
    [InlineData("-170141183460469231731687303715884105729")]    // the smallest HUGEINT - 1
    [InlineData("340282366920938463463374607431768211456")]     // 2^128: one bit more than an Int128 has
    [InlineData("-340282366920938463463374607431768211456")]
    public void HugeIntRejectsAValueOutOfRange(string text)
    {
        var value = BigInteger.Parse(text, CultureInfo.InvariantCulture);

        FluentActions.Invoking(() => new DuckDBHugeInt(value)).Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("value must be between -170141183460469231731687303715884105728 and 170141183460469231731687303715884105727*")
            .Which.ParamName.Should().Be("value");
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("-170141183460469231731687303715884105728")]    // fits an Int128, and is still not a UHUGEINT
    [InlineData("340282366920938463463374607431768211456")]     // the largest UHUGEINT + 1
    public void UHugeIntRejectsAValueOutOfRange(string text)
    {
        var value = BigInteger.Parse(text, CultureInfo.InvariantCulture);

        FluentActions.Invoking(() => new DuckDBUHugeInt(value)).Should().Throw<ArgumentOutOfRangeException>()
            .WithMessage("value must be between 0 and 340282366920938463463374607431768211455*")
            .Which.ParamName.Should().Be("value");
    }

    private static BigInteger Limit(Assembly assembly, Type type, string property)
    {
        return (BigInteger)assembly.GetType(type.FullName).GetProperty(property).GetValue(null);
    }
}
