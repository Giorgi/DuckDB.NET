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

    private static BigInteger Limit(Assembly assembly, Type type, string property)
    {
        return (BigInteger)assembly.GetType(type.FullName).GetProperty(property).GetValue(null);
    }
}
