using System.Globalization;
using System.Reflection;
using System.Runtime.Loader;

namespace DuckDB.NET.Test;

public class DuckDBHugeIntTests
{
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

    // The constructor splits a value into its two halves by division, and ToBigInteger puts them together
    // through Int128. The two do not share code, so a value that survives both was converted correctly.
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

        new DuckDBHugeInt(value).ToBigInteger().Should().Be(value);
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

        new DuckDBUHugeInt(value).ToBigInteger().Should().Be(value);
    }

    private static BigInteger Limit(Assembly assembly, Type type, string property)
    {
        return (BigInteger)assembly.GetType(type.FullName).GetProperty(property).GetValue(null);
    }
}
