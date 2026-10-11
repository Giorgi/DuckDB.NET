using System.Globalization;
using System.Reflection;

namespace DuckDB.NET.Test;

// Pins how each numeric column type reads as each numeric .NET type: the value when the read is supported and
// the value fits, InvalidCastException otherwise. The rules below describe the reader as it is, including the
// reads it refuses, such as a HUGEINT column read as double.
public class NumericReaderConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private static readonly string[] IntegerColumns = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"];
    private static readonly string[] HugeIntColumns = ["HUGEINT", "UHUGEINT"];

    private static readonly Type[] IntegerTargets = [typeof(sbyte), typeof(short), typeof(int), typeof(long), typeof(byte), typeof(ushort), typeof(uint), typeof(ulong)];
    private static readonly Type[] Targets = [.. IntegerTargets, typeof(float), typeof(double), typeof(decimal), typeof(BigInteger)];

    private static readonly Dictionary<string, Type> ColumnClrTypes = new()
    {
        ["TINYINT"] = typeof(sbyte), ["SMALLINT"] = typeof(short), ["INTEGER"] = typeof(int), ["BIGINT"] = typeof(long),
        ["UTINYINT"] = typeof(byte), ["USMALLINT"] = typeof(ushort), ["UINTEGER"] = typeof(uint), ["UBIGINT"] = typeof(ulong),
        ["FLOAT"] = typeof(float), ["DOUBLE"] = typeof(double), ["HUGEINT"] = typeof(BigInteger), ["UHUGEINT"] = typeof(BigInteger),
    };

    private static readonly MethodInfo GetFieldValueMethod = typeof(DuckDBDataReader).GetMethod(nameof(DuckDBDataReader.GetFieldValue));

    public static IEnumerable<object[]> Cases()
    {
        foreach (var column in ColumnClrTypes.Keys)
        {
            foreach (var value in new[] { "0", "42", "-1", "127", "128", "255", "300", "70000", "5000000000" })
            {
                yield return [column, value];
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ReadNumericColumnAsEveryNumericType(string column, string literal)
    {
        Command.CommandText = $"SELECT TRY_CAST({literal} AS {column}) AS c";
        using var reader = Command.ExecuteReader();
        reader.Read();

        if (reader.IsDBNull(0))
        {
            // The column type cannot hold this value, so there is nothing to read.
            return;
        }

        var value = BigInteger.Parse(literal, CultureInfo.InvariantCulture);
        var expected = (decimal)value;

        reader.GetValue(0).Should().BeOfType(ColumnClrTypes[column]);
        AsDecimal(reader.GetValue(0)).Should().Be(expected);

        foreach (var target in Targets)
        {
            var expectValue = IsSupported(column, target) && Fits(value, target);

            foreach (var requested in new[] { target, typeof(Nullable<>).MakeGenericType(target) })
            {
                var read = () => GetFieldValueMethod.MakeGenericMethod(requested).Invoke(reader, [0]);

                if (expectValue)
                {
                    AsDecimal(read()).Should().Be(expected, $"{column} read as {requested}");
                }
                else
                {
                    read.Should().Throw<TargetInvocationException>($"{column} read as {requested}").WithInnerException<InvalidCastException>();
                }
            }
        }

        // The named getters go the same way as GetFieldValue<T>.
        Check(() => reader.GetByte(0), typeof(byte));
        Check(() => reader.GetInt16(0), typeof(short));
        Check(() => reader.GetInt32(0), typeof(int));
        Check(() => reader.GetInt64(0), typeof(long));
        Check(() => reader.GetFloat(0), typeof(float));
        Check(() => reader.GetDouble(0), typeof(double));
        Check(() => reader.GetDecimal(0), typeof(decimal));

        void Check<T>(Func<T> read, Type target)
        {
            if (IsSupported(column, target) && Fits(value, target))
            {
                AsDecimal(read()).Should().Be(expected, $"{column} read with the {target.Name} getter");
            }
            else
            {
                read.Should().Throw<InvalidCastException>($"{column} read with the {target.Name} getter");
            }
        }
    }

    [Fact]
    public void OverflowingIntegerReadNamesBothTypesAndTheColumn()
    {
        Command.CommandText = "SELECT 300::SMALLINT AS small, 5000000000::BIGINT AS big, (-1)::INTEGER AS negative";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetByte(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from Int16 to Byte in column small");
        reader.Invoking(r => r.GetInt32(1)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from Int64 to Int32 in column big");
        reader.Invoking(r => r.GetFieldValue<ulong>(2)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from Int32 to UInt64 in column negative");
    }

    // HUGEINT, UHUGEINT and VARINT are named by the type they are read as, BigInteger, and not by the struct the
    // vector holds.
    [Fact]
    public void OverflowingBigIntegerReadNamesBigIntegerAndTheColumn()
    {
        Command.CommandText = "SELECT 10000000000000000000000::HUGEINT AS huge, 10000000000000000000000::UHUGEINT AS unsigned, " +
                              "10000000000000000000000::VARINT AS big, (-1)::HUGEINT AS negative";
        using var reader = Command.ExecuteReader();
        reader.Read();

        foreach (var name in new[] { "huge", "unsigned", "big" })
        {
            var ordinal = reader.GetOrdinal(name);
            var message = $"Cannot cast from BigInteger to Int64 in column {name}";

            reader.Invoking(r => r.GetInt64(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<long>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<long?>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
        }

        reader.Invoking(r => r.GetFieldValue<ulong>(3)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from BigInteger to UInt64 in column negative");
    }

    [Fact]
    public void FloatingPointColumnsKeepTheirFraction()
    {
        Command.CommandText = "SELECT 1.5::FLOAT, 1.5::DOUBLE, 2.5::DOUBLE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFloat(0).Should().Be(1.5f);
        reader.GetDouble(1).Should().Be(1.5);
        reader.GetFieldValue<float?>(0).Should().Be(1.5f);
        reader.GetFieldValue<double?>(1).Should().Be(1.5);

        // A FLOAT widens to double, a DOUBLE narrows to float, and both read as decimal.
        reader.GetDouble(0).Should().Be(1.5);
        reader.GetFieldValue<double?>(0).Should().Be(1.5);
        reader.GetFloat(1).Should().Be(1.5f);
        reader.GetFieldValue<float?>(1).Should().Be(1.5f);
        reader.GetDecimal(0).Should().Be(1.5m);
        reader.GetDecimal(1).Should().Be(1.5m);

        // Read as an integer, a fraction is rounded to the nearest even number.
        reader.GetInt32(0).Should().Be(2);
        reader.GetInt32(1).Should().Be(2);
        reader.GetInt32(2).Should().Be(2);
        reader.GetInt64(1).Should().Be(2);
    }

    [Fact]
    public void SmallestAndLargestValueOfEachIntegerType()
    {
        Command.CommandText = "SELECT (-128)::TINYINT, 127::TINYINT, (-32768)::SMALLINT, 32767::SMALLINT, (-2147483648)::INTEGER, 2147483647::INTEGER, " +
                              "(-9223372036854775808)::BIGINT, 9223372036854775807::BIGINT, 255::UTINYINT, 65535::USMALLINT, 4294967295::UINTEGER, 18446744073709551615::UBIGINT";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<sbyte>(0).Should().Be(sbyte.MinValue);
        reader.GetFieldValue<sbyte>(1).Should().Be(sbyte.MaxValue);
        reader.GetInt16(2).Should().Be(short.MinValue);
        reader.GetInt16(3).Should().Be(short.MaxValue);
        reader.GetInt32(4).Should().Be(int.MinValue);
        reader.GetInt32(5).Should().Be(int.MaxValue);
        reader.GetInt64(6).Should().Be(long.MinValue);
        reader.GetInt64(7).Should().Be(long.MaxValue);
        reader.GetByte(8).Should().Be(byte.MaxValue);
        reader.GetFieldValue<ushort>(9).Should().Be(ushort.MaxValue);
        reader.GetFieldValue<uint>(10).Should().Be(uint.MaxValue);
        reader.GetFieldValue<ulong>(11).Should().Be(ulong.MaxValue);

        // One step wider holds the value; one step narrower does not.
        reader.GetInt64(4).Should().Be(int.MinValue);
        reader.GetInt64(10).Should().Be(uint.MaxValue);
        reader.GetFieldValue<BigInteger>(11).Should().Be(new BigInteger(ulong.MaxValue));
        reader.Invoking(r => r.GetInt16(4)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt32(7)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt32(10)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(11)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<sbyte>(8)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void VarIntReadsAsIntegersAndBigIntegerOnly()
    {
        Command.CommandText = "SELECT 42::VARINT, (-5)::VARINT, 99999999999999999999::VARINT";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetValue(0).Should().Be(new BigInteger(42));
        reader.GetFieldValue<BigInteger>(1).Should().Be(new BigInteger(-5));
        reader.GetFieldValue<BigInteger>(2).Should().Be(BigInteger.Parse("99999999999999999999"));
        reader.GetFieldValue<sbyte>(0).Should().Be(42);
        reader.GetInt32(0).Should().Be(42);
        reader.GetInt64(1).Should().Be(-5);
        reader.GetFieldValue<ulong>(0).Should().Be(42);

        reader.Invoking(r => r.GetFieldValue<ulong>(1)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(2)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFloat(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetDouble(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetDecimal(0)).Should().Throw<InvalidCastException>();
    }

    [Theory]
    [InlineData("DECIMAL(4,0)")]
    [InlineData("DECIMAL(9,2)")]
    [InlineData("DECIMAL(18,2)")]
    [InlineData("DECIMAL(38,2)")]
    public void DecimalIsRefusedAsIntegerFloatAndDouble(string type)
    {
        Command.CommandText = $"SELECT 42::{type}";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDecimal(0).Should().Be(42m);
        reader.GetFieldValue<decimal?>(0).Should().Be(42m);
        reader.GetValue(0).Should().Be(42m);

        reader.Invoking(r => r.GetInt32(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetDouble(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFloat(0)).Should().Throw<InvalidCastException>();
    }

    // The items of a list come from the same readers as a column of that type.
    [Fact]
    public void ListsOfNumbers()
    {
        Command.CommandText = "SELECT [1,2,3]::INTEGER[], [1,NULL,3]::INTEGER[], [300,2]::INTEGER[], [1,2]::BIGINT[], [1.5,2.5]::DOUBLE[], " +
                              "[true,false]::BOOLEAN[], [1,2]::HUGEINT[], [[1,2],[3]]::INTEGER[][], [1.5,2.5]::FLOAT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<int>>(0).Should().Equal(1, 2, 3);
        reader.GetFieldValue<List<long>>(0).Should().Equal(1L, 2L, 3L);
        reader.GetFieldValue<List<short>>(0).Should().Equal((short)1, (short)2, (short)3);
        reader.GetFieldValue<List<double>>(0).Should().Equal(1d, 2d, 3d);
        reader.GetFieldValue<List<int?>>(0).Should().Equal(1, 2, 3);
        reader.GetValue(0).Should().BeOfType<List<int>>().Which.Should().Equal(1, 2, 3);

        reader.GetFieldValue<List<int?>>(1).Should().Equal(1, null, 3);
        reader.GetFieldValue<List<long?>>(1).Should().Equal(1L, null, 3L);
        reader.Invoking(r => r.GetFieldValue<List<int>>(1)).Should().Throw<InvalidCastException>();

        reader.GetFieldValue<List<long>>(2).Should().Equal(300L, 2L);
        reader.Invoking(r => r.GetFieldValue<List<byte>>(2)).Should().Throw<InvalidCastException>();

        reader.GetFieldValue<List<long>>(3).Should().Equal(1L, 2L);
        reader.GetFieldValue<List<int>>(3).Should().Equal(1, 2);

        reader.GetFieldValue<List<double>>(4).Should().Equal(1.5, 2.5);
        reader.GetFieldValue<List<decimal>>(4).Should().Equal(1.5m, 2.5m);
        reader.GetFieldValue<List<float>>(4).Should().Equal(1.5f, 2.5f);

        reader.GetFieldValue<List<bool>>(5).Should().Equal(true, false);
        reader.GetFieldValue<List<bool?>>(5).Should().Equal(true, false);

        reader.GetFieldValue<List<BigInteger>>(6).Should().Equal(new BigInteger(1), new BigInteger(2));

        var nested = reader.GetFieldValue<List<List<int>>>(7);
        nested.Should().HaveCount(2);
        nested[0].Should().Equal(1, 2);
        nested[1].Should().Equal(3);
        reader.GetFieldValue<List<List<long>>>(7)[0].Should().Equal(1L, 2L);

        reader.GetFieldValue<List<float>>(8).Should().Equal(1.5f, 2.5f);
        reader.GetFieldValue<List<double>>(8).Should().Equal(1.5, 2.5);
        reader.GetFieldValue<List<decimal>>(8).Should().Equal(1.5m, 2.5m);
    }

    [Fact]
    public void MapsAndStructsOfNumbers()
    {
        Command.CommandText = "SELECT MAP {1::INTEGER: 10::BIGINT}, {'a': 1::INTEGER, 'b': 2::BIGINT}";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<Dictionary<int, long>>(0).Should().Equal(new Dictionary<int, long> { [1] = 10 });
        reader.GetFieldValue<Dictionary<long, int>>(0).Should().Equal(new Dictionary<long, int> { [1] = 10 });
        reader.GetFieldValue<Dictionary<int, long?>>(0).Should().Equal(new Dictionary<int, long?> { [1] = 10 });

        var fields = reader.GetFieldValue<Dictionary<string, object>>(1);
        fields["a"].Should().Be(1);
        fields["b"].Should().Be(2L);
    }

    // The values of a map and the fields of a struct are converted on another path than a column is.
    // FLOAT, DOUBLE and decimal convert the same way on both.
    [Fact]
    public void MapsAndStructsConvertFloatingPointLikeColumns()
    {
        Command.CommandText = "SELECT MAP {1::INTEGER: 1.5::DOUBLE} AS doubles, MAP {1::INTEGER: 2.5::FLOAT} AS floats, " +
                              "{'single': 2.5::FLOAT, 'double': 1.5::DOUBLE} AS fields";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<Dictionary<int, decimal>>(0).Should().Equal(new Dictionary<int, decimal> { [1] = 1.5m });
        reader.GetFieldValue<Dictionary<int, float>>(0).Should().Equal(new Dictionary<int, float> { [1] = 1.5f });
        reader.GetFieldValue<Dictionary<int, decimal>>(1).Should().Equal(new Dictionary<int, decimal> { [1] = 2.5m });
        reader.GetFieldValue<Dictionary<int, double>>(1).Should().Equal(new Dictionary<int, double> { [1] = 2.5 });

        var asDecimals = reader.GetFieldValue<FloatingFields<decimal, decimal>>(2);
        asDecimals.Single.Should().Be(2.5m);
        asDecimals.Double.Should().Be(1.5m);

        var swapped = reader.GetFieldValue<FloatingFields<double, float>>(2);
        swapped.Single.Should().Be(2.5);
        swapped.Double.Should().Be(1.5f);
    }

    private class FloatingFields<TSingle, TDouble>
    {
        public TSingle Single { get; set; }
        public TDouble Double { get; set; }
    }

    // Integers that float or double cannot hold exactly, each with the result of the C# cast of the same value.
    // 1152921573326323713 is 2^60 + 2^36 + 1. As a BIGINT it rounds up as a float, and down when it goes through
    // double first, so that row tells the two conversions apart. The UBIGINT rows only pin the result of the
    // runtime's own cast: .NET 8 converts a ulong to float through double, later versions convert it directly.
    public static IEnumerable<object[]> InexactCases()
    {
        var asInt = 16777217;
        var asUInt = 16777217u;

        yield return ["INTEGER", "16777217", (float)asInt, (double)asInt, (decimal)asInt];
        yield return ["UINTEGER", "16777217", (float)asUInt, (double)asUInt, (decimal)asUInt];

        foreach (var signed in new[] { 16777217L, 9007199254740993L, 1152921573326323713L, long.MaxValue })
        {
            var unsigned = (ulong)signed;
            var literal = signed.ToString(CultureInfo.InvariantCulture);

            yield return ["BIGINT", literal, (float)signed, (double)signed, (decimal)signed];
            yield return ["UBIGINT", literal, (float)unsigned, (double)unsigned, (decimal)unsigned];
        }

        var largest = ulong.MaxValue;
        yield return ["UBIGINT", "18446744073709551615", (float)largest, (double)largest, (decimal)largest];
    }

    [Theory]
    [MemberData(nameof(InexactCases))]
    public void IntegerReadAsFloatDoubleOrDecimalRoundsLikeTheCSharpCast(string column, string literal, float asFloat, double asDouble, decimal asDecimal)
    {
        Command.CommandText = $"SELECT {literal}::{column} AS c";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFloat(0).Should().Be(asFloat);
        reader.GetFieldValue<float>(0).Should().Be(asFloat);
        reader.GetFieldValue<float?>(0).Should().Be(asFloat);

        reader.GetDouble(0).Should().Be(asDouble);
        reader.GetFieldValue<double>(0).Should().Be(asDouble);
        reader.GetFieldValue<double?>(0).Should().Be(asDouble);

        reader.GetDecimal(0).Should().Be(asDecimal);
        reader.GetFieldValue<decimal>(0).Should().Be(asDecimal);
        reader.GetFieldValue<decimal?>(0).Should().Be(asDecimal);
    }

    [Fact]
    public void FloatReadAsDoubleIsAPlainWidening()
    {
        Command.CommandText = "SELECT 0.1::FLOAT, 'NaN'::FLOAT, 'Infinity'::FLOAT, '-Infinity'::FLOAT, '-0.0'::FLOAT, [0.1, 0.7]::FLOAT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // The double nearest to the float 0.1, which is not the double 0.1.
        reader.GetDouble(0).Should().Be((double)0.1f).And.Be(0.10000000149011612).And.NotBe(0.1);
        reader.GetFieldValue<double>(0).Should().Be((double)0.1f);
        reader.GetFieldValue<double?>(0).Should().Be((double)0.1f);

        double.IsNaN(reader.GetDouble(1)).Should().BeTrue();
        double.IsNaN(reader.GetFieldValue<double?>(1).Value).Should().BeTrue();
        reader.GetDouble(2).Should().Be(double.PositiveInfinity);
        reader.GetFieldValue<double?>(2).Should().Be(double.PositiveInfinity);
        reader.GetDouble(3).Should().Be(double.NegativeInfinity);
        reader.GetFieldValue<double?>(3).Should().Be(double.NegativeInfinity);

        reader.GetDouble(4).Should().Be(0d);
        double.IsNegative(reader.GetDouble(4)).Should().BeTrue();
        double.IsNegative(reader.GetFieldValue<double?>(4).Value).Should().BeTrue();

        reader.GetFieldValue<List<double>>(5).Should().Equal((double)0.1f, (double)0.7f);
    }

    // The C# cast, with no check: digits beyond what a float holds are lost, and a value too large for a float
    // reads as infinity. Npgsql does the same.
    [Fact]
    public void DoubleReadAsFloatIsAPlainNarrowing()
    {
        Command.CommandText = "SELECT 0.1::DOUBLE, 123456789.123456789::DOUBLE, 1e300::DOUBLE, (-1e300)::DOUBLE, 1e-300::DOUBLE, " +
                              "'NaN'::DOUBLE, 'Infinity'::DOUBLE, '-0.0'::DOUBLE, [0.1, 1e300]::DOUBLE[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFloat(0).Should().Be(0.1f);
        reader.GetFieldValue<float>(0).Should().Be(0.1f);
        reader.GetFieldValue<float?>(0).Should().Be(0.1f);

        reader.GetFloat(1).Should().Be((float)123456789.123456789d).And.Be(123456792f);

        reader.GetFloat(2).Should().Be(float.PositiveInfinity);
        reader.GetFieldValue<float?>(2).Should().Be(float.PositiveInfinity);
        reader.GetFloat(3).Should().Be(float.NegativeInfinity);
        reader.GetFloat(4).Should().Be(0f);

        float.IsNaN(reader.GetFloat(5)).Should().BeTrue();
        reader.GetFloat(6).Should().Be(float.PositiveInfinity);
        float.IsNegative(reader.GetFloat(7)).Should().BeTrue();

        reader.GetFieldValue<List<float>>(8).Should().Equal(0.1f, float.PositiveInfinity);
    }

    [Theory]
    [InlineData("FLOAT", "Single")]
    [InlineData("DOUBLE", "Double")]
    public void FloatingPointColumnReadAsInteger(string column, string clrName)
    {
        Command.CommandText = $"SELECT (-0.5)::{column}, (-1.5)::{column}, (-2.5)::{column}, 0.5::{column}, 3.5::{column}, (-3.5)::{column}, " +
                              $"'NaN'::{column} AS nan, 'Infinity'::{column} AS inf, '-Infinity'::{column} AS neginf, 1e10::{column} AS big";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // A half goes to the nearest even number, on both sides of zero.
        var expected = new[] { 0, -2, -2, 0, 4, -4 };

        for (var ordinal = 0; ordinal < expected.Length; ordinal++)
        {
            reader.GetInt32(ordinal).Should().Be(expected[ordinal], $"column {ordinal}");
            reader.GetInt64(ordinal).Should().Be(expected[ordinal], $"column {ordinal}");
            reader.GetFieldValue<int?>(ordinal).Should().Be(expected[ordinal], $"column {ordinal}");
            reader.GetFieldValue<short>(ordinal).Should().Be((short)expected[ordinal], $"column {ordinal}");
        }

        foreach (var name in new[] { "nan", "inf", "neginf", "big" })
        {
            var ordinal = reader.GetOrdinal(name);
            var message = $"Cannot cast from {clrName} to Int32 in column {name}";

            reader.Invoking(r => r.GetInt32(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<int>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<int?>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
        }

        // 1e10 is too large for an int only.
        reader.GetInt64(reader.GetOrdinal("big")).Should().Be(10_000_000_000L);
    }

    // Each target type has its own range and its own rounding point, so a read as one integer type must not
    // go through another.
    [Fact]
    public void DoubleColumnReadAsUnsignedLongLongAndByte()
    {
        Command.CommandText = "SELECT (-0.5)::DOUBLE AS neghalf, (-1.5)::DOUBLE AS negative, 1e19::DOUBLE AS huge, " +
                              "255.5::DOUBLE AS above, 254.5::DOUBLE AS below, 'NaN'::DOUBLE AS nan";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // -0.5 rounds to zero, which every type holds.
        reader.GetFieldValue<ulong>(0).Should().Be(0UL);
        reader.GetInt64(0).Should().Be(0L);
        reader.GetByte(0).Should().Be(0);

        // -1.5 rounds to -2.
        reader.GetInt64(1).Should().Be(-2L);
        Refused<ulong>("negative");
        Refused<byte>("negative");

        // 1e19 is above the largest long and below the largest ulong.
        reader.GetFieldValue<ulong>(2).Should().Be(10_000_000_000_000_000_000UL);
        reader.GetFieldValue<ulong?>(2).Should().Be(10_000_000_000_000_000_000UL);
        Refused<long>("huge");
        Refused<byte>("huge");

        // 255.5 rounds to 256, one more than a byte holds. 254.5 rounds to 254.
        reader.GetFieldValue<ulong>(3).Should().Be(256UL);
        reader.GetInt64(3).Should().Be(256L);
        Refused<byte>("above");
        reader.GetByte(4).Should().Be(254);
        reader.GetFieldValue<byte?>(4).Should().Be((byte)254);
        reader.GetInt64(4).Should().Be(254L);

        Refused<ulong>("nan");
        Refused<long>("nan");
        Refused<byte>("nan");

        void Refused<T>(string name)
        {
            var ordinal = reader.GetOrdinal(name);

            reader.Invoking(r => r.GetFieldValue<T>(ordinal)).Should().Throw<InvalidCastException>()
                .WithMessage($"Cannot cast from Double to {typeof(T).Name} in column {name}");
        }
    }

    // The conversion is the C# cast: a float keeps 7 significant digits and a double 15, so 0.1 reads as 0.1.
    [Theory]
    [InlineData("FLOAT", "Single")]
    [InlineData("DOUBLE", "Double")]
    public void FloatingPointColumnReadAsDecimal(string column, string clrName)
    {
        Command.CommandText = $"SELECT 1.5::{column}, 0.1::{column}, (-123.456)::{column}, 1e20::{column}, " +
                              $"'NaN'::{column} AS nan, 'Infinity'::{column} AS inf, '-Infinity'::{column} AS neginf, 1e30::{column} AS big";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDecimal(0).Should().Be(1.5m);
        reader.GetDecimal(1).Should().Be(0.1m);
        reader.GetDecimal(2).Should().Be(-123.456m);
        reader.GetDecimal(3).Should().Be(100000000000000000000m);
        reader.GetFieldValue<decimal>(1).Should().Be(0.1m);
        reader.GetFieldValue<decimal?>(1).Should().Be(0.1m);

        // A decimal has no NaN and no infinity, and holds nothing above about 7.9e28.
        foreach (var name in new[] { "nan", "inf", "neginf", "big" })
        {
            var ordinal = reader.GetOrdinal(name);
            var message = $"Cannot cast from {clrName} to Decimal in column {name}";

            reader.Invoking(r => r.GetDecimal(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<decimal>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
            reader.Invoking(r => r.GetFieldValue<decimal?>(ordinal)).Should().Throw<InvalidCastException>().WithMessage(message);
        }
    }

    // A UNION column has no reader. The query runs, and every read of a non-null value says so.
    [Fact]
    public void ColumnTypeWithoutAReaderThrows()
    {
        Command.CommandText = "SELECT union_value(a := 1) AS c";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        const string message = "Unrecognised type Union (28) for column c";

        reader.Invoking(r => r.GetValue(0)).Should().Throw<ArgumentException>().WithMessage(message);
        reader.Invoking(r => r.GetFieldValue<int>(0)).Should().Throw<ArgumentException>().WithMessage(message);
        reader.Invoking(r => r.GetFieldType(0)).Should().Throw<ArgumentException>().WithMessage(message);
    }

    // A chunk holds 2048 rows, so 5000 rows make the readers move to a new chunk twice.
    // Every SQL expression below has an exact result, which the test computes again from i.
    // A materialized result can give the next chunk the address of the previous one, and a reader that was not
    // moved to the new chunk then still reads the right values. A streaming result was not seen to do that.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadersKeepConvertingAcrossChunks(bool useStreamingMode)
    {
        const int rowCount = 5000;
        const decimal step = 1234567890123.0000000001m;

        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "SELECT i, " +
                              "CASE WHEN i % 7 = 0 THEN NULL ELSE (i - 2500)::INTEGER END AS whole, " +
                              "CASE WHEN i % 11 = 0 THEN NULL ELSE i::UBIGINT * 3600000000000000::UBIGINT END AS unsigned, " +
                              "CASE WHEN i % 3 = 0 THEN NULL ELSE ((i - 2500)::DOUBLE / 8)::FLOAT END AS fraction, " +
                              "d, " +
                              "CASE WHEN i % 13 = 0 THEN NULL ELSE [d, -d, NULL] END AS items, " +
                              "{'d': d, 'i': i::INTEGER} AS fields, " +
                              "CASE WHEN i % 17 = 0 THEN NULL ELSE (i - 2500)::HUGEINT * 10000000000000000000000::HUGEINT END AS huge, " +
                              "CASE WHEN i % 19 = 0 THEN NULL ELSE ((i - 2500)::HUGEINT * 10000000000000000000000::HUGEINT)::VARINT END AS big " +
                              "FROM (SELECT i, CASE WHEN i % 5 = 0 THEN NULL " +
                              "ELSE ((i - 2500)::DECIMAL(5,0) * 1234567890123.0000000001::DECIMAL(23,10))::DECIMAL(38,10) END AS d " +
                              $"FROM range(0, {rowCount}) t(i)) ORDER BY i";
        using var reader = Command.ExecuteReader();

        reader.GetFieldType(1).Should().Be(typeof(int));
        reader.GetFieldType(2).Should().Be(typeof(ulong));
        reader.GetFieldType(3).Should().Be(typeof(float));
        reader.GetFieldType(4).Should().Be(typeof(decimal));
        reader.GetFieldType(7).Should().Be(typeof(BigInteger));
        reader.GetFieldType(8).Should().Be(typeof(BigInteger));

        var hugeStep = BigInteger.Pow(10, 22);
        var row = 0;

        while (reader.Read())
        {
            reader.GetInt64(0).Should().Be(row);

            int? whole = row % 7 == 0 ? null : row - 2500;
            decimal? unsigned = row % 11 == 0 ? null : (decimal)((ulong)row * 3600000000000000UL);
            double? fraction = row % 3 == 0 ? null : (double)((row - 2500) / 8f);
            decimal? d = row % 5 == 0 ? null : (row - 2500) * step;

            reader.GetFieldValue<long?>(1).Should().Be(whole, $"row {row}");
            reader.GetFieldValue<double?>(1).Should().Be(whole, $"row {row}");
            reader.GetFieldValue<decimal?>(2).Should().Be(unsigned, $"row {row}");
            reader.GetFieldValue<double?>(3).Should().Be(fraction, $"row {row}");
            reader.GetFieldValue<decimal?>(4).Should().Be(d, $"row {row}");

            if (whole.HasValue)
            {
                reader.GetInt64(1).Should().Be(whole.Value, $"row {row}");
                reader.GetDouble(1).Should().Be(whole.Value, $"row {row}");
            }

            if (unsigned.HasValue)
            {
                reader.GetDecimal(2).Should().Be(unsigned.Value, $"row {row}");
            }

            if (fraction.HasValue)
            {
                reader.GetDouble(3).Should().Be(fraction.Value, $"row {row}");
            }

            if (d.HasValue)
            {
                reader.GetDecimal(4).Should().Be(d.Value, $"row {row}");
                reader.GetValue(4).Should().Be(d.Value, $"row {row}");
            }

            if (row % 13 == 0)
            {
                reader.IsDBNull(5).Should().BeTrue($"row {row}");
            }
            else
            {
                reader.GetFieldValue<List<decimal?>>(5).Should().Equal([d, -d, null], $"row {row}");
            }

            var fields = reader.GetFieldValue<Dictionary<string, object>>(6);
            fields["d"].Should().Be(d, $"row {row}");
            fields["i"].Should().Be(row, $"row {row}");

            // Only row 2500 holds a value that fits a long: zero.
            BigInteger? huge = row % 17 == 0 ? null : (row - 2500) * hugeStep;
            BigInteger? big = row % 19 == 0 ? null : (row - 2500) * hugeStep;

            reader.GetFieldValue<BigInteger?>(7).Should().Be(huge, $"row {row}");
            reader.GetFieldValue<BigInteger?>(8).Should().Be(big, $"row {row}");

            if (huge.HasValue)
            {
                reader.GetFieldValue<BigInteger>(7).Should().Be(huge.Value, $"row {row}");
                reader.GetValue(7).Should().Be(huge.Value, $"row {row}");
                reader.GetFieldValue<DuckDBHugeInt>(7).ToBigInteger().Should().Be(huge.Value, $"row {row}");

                if (row == 2500)
                {
                    reader.GetInt64(7).Should().Be(0);
                }
                else
                {
                    reader.Invoking(r => r.GetInt64(7)).Should().Throw<InvalidCastException>($"row {row}");
                }
            }

            if (big.HasValue)
            {
                reader.GetFieldValue<BigInteger>(8).Should().Be(big.Value, $"row {row}");
                reader.GetValue(8).Should().Be(big.Value, $"row {row}");

                if (row == 2500)
                {
                    reader.GetInt64(8).Should().Be(0);
                }
            }

            row++;
        }

        row.Should().Be(rowCount);
    }

    // Every value used here is a whole number that float, double and decimal hold exactly.
    private static decimal AsDecimal(object value) => value is BigInteger big ? (decimal)big : Convert.ToDecimal(value, CultureInfo.InvariantCulture);

    // What the reader converts at all, whatever the value.
    private static bool IsSupported(string column, Type target)
    {
        if (Array.IndexOf(IntegerTargets, target) >= 0)
        {
            return true;
        }

        if (Array.IndexOf(IntegerColumns, column) >= 0)
        {
            return true;
        }

        if (Array.IndexOf(HugeIntColumns, column) >= 0)
        {
            return target == typeof(BigInteger);
        }

        // FLOAT and DOUBLE are read as an integer, as each other and as decimal, and not as BigInteger.
        return target != typeof(BigInteger);
    }

    private static bool Fits(BigInteger value, Type target)
    {
        if (target == typeof(sbyte)) return value >= sbyte.MinValue && value <= sbyte.MaxValue;
        if (target == typeof(short)) return value >= short.MinValue && value <= short.MaxValue;
        if (target == typeof(int)) return value >= int.MinValue && value <= int.MaxValue;
        if (target == typeof(long)) return value >= long.MinValue && value <= long.MaxValue;
        if (target == typeof(byte)) return value >= byte.MinValue && value <= byte.MaxValue;
        if (target == typeof(ushort)) return value >= ushort.MinValue && value <= ushort.MaxValue;
        if (target == typeof(uint)) return value >= uint.MinValue && value <= uint.MaxValue;
        if (target == typeof(ulong)) return value >= ulong.MinValue && value <= ulong.MaxValue;

        return true;
    }
}
