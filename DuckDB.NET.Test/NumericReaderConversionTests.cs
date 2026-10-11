using System.Globalization;
using System.Reflection;

namespace DuckDB.NET.Test;

// Pins how each numeric column type reads as each numeric .NET type: the value when the read is supported and
// the value fits, InvalidCastException otherwise. The rules below describe the reader as it is, including the
// reads it refuses, such as a HUGEINT column read as double.
//
// Order of the tests: every column type against every .NET type, integer columns, FLOAT and DOUBLE columns,
// HUGEINT, VARINT and DECIMAL columns, lists, maps and structs, a column type without a reader, results of
// several chunks. The helpers of the first test are at the end.
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

    // Reads one value from a column of one type as every .NET type in the table, and expects:
    //
    //                       | sbyte, short, int, long,   | float, double,       |
    //   Column type         | byte, ushort, uint, ulong  | decimal              | BigInteger
    //   --------------------+----------------------------+----------------------+---------------------
    //   TINYINT to UBIGINT  | the value, if it fits      | the value            | the value
    //   FLOAT, DOUBLE       | the value, if it fits      | the value            | InvalidCastException
    //   HUGEINT, UHUGEINT   | the value, if it fits      | InvalidCastException | the value
    //
    // "If it fits" means the value is in the range of the .NET type. If it is not, the read throws
    // InvalidCastException. "TINYINT to UBIGINT" is the eight signed and unsigned integer column types.
    // IsSupported holds the table and Fits holds the ranges; both are at the end of the file.
    //
    // Each .NET type T is read as GetFieldValue<T> and as GetFieldValue<T?>, and both must go the same way.
    // GetByte, GetInt16, GetInt32, GetInt64, GetFloat, GetDouble and GetDecimal must go the way of their type.
    // GetValue must return the value in the .NET type that ColumnClrTypes gives for the column type.
    [Theory]
    [MemberData(nameof(Cases))]
    public void ReadNumericColumnAsEveryNumericType(string column, string literal)
    {
        Command.CommandText = $"SELECT TRY_CAST({literal} AS {column}) AS c";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // TRY_CAST gives NULL when the column type cannot hold the value, for example 300 in a TINYINT column
        // or -1 in a UBIGINT column. Such a case has no value to read, so it ends here and checks nothing.
        var columnTypeCannotHoldTheValue = reader.IsDBNull(0);

        if (columnTypeCannotHoldTheValue)
        {
            return;
        }

        var value = BigInteger.Parse(literal, CultureInfo.InvariantCulture);
        var expected = (decimal)value;

        reader.GetValue(0).Should().BeOfType(ColumnClrTypes[column]);
        AsDecimal(reader.GetValue(0)).Should().Be(expected);

        foreach (var target in Targets)
        {
            var expectValue = IsSupported(column, target) && Fits(value, target);

            // The type itself and its nullable form, for example int and int?.
            foreach (var requested in new[] { target, typeof(Nullable<>).MakeGenericType(target) })
            {
                // Calls reader.GetFieldValue<requested>(0). An exception from it arrives wrapped in a
                // TargetInvocationException, because the call is made through reflection.
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
    public void SmallestAndLargestValueOfEachIntegerType()
    {
        Command.CommandText = """
            SELECT (-128)::TINYINT AS tinyint_min,
                   127::TINYINT AS tinyint_max,
                   (-32768)::SMALLINT AS smallint_min,
                   32767::SMALLINT AS smallint_max,
                   (-2147483648)::INTEGER AS integer_min,
                   2147483647::INTEGER AS integer_max,
                   (-9223372036854775808)::BIGINT AS bigint_min,
                   9223372036854775807::BIGINT AS bigint_max,
                   255::UTINYINT AS utinyint_max,
                   65535::USMALLINT AS usmallint_max,
                   4294967295::UINTEGER AS uinteger_max,
                   18446744073709551615::UBIGINT AS ubigint_max
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var tinyIntMin = reader.GetOrdinal("tinyint_min");
        var tinyIntMax = reader.GetOrdinal("tinyint_max");
        var smallIntMin = reader.GetOrdinal("smallint_min");
        var smallIntMax = reader.GetOrdinal("smallint_max");
        var integerMin = reader.GetOrdinal("integer_min");
        var integerMax = reader.GetOrdinal("integer_max");
        var bigIntMin = reader.GetOrdinal("bigint_min");
        var bigIntMax = reader.GetOrdinal("bigint_max");
        var uTinyIntMax = reader.GetOrdinal("utinyint_max");
        var uSmallIntMax = reader.GetOrdinal("usmallint_max");
        var uIntegerMax = reader.GetOrdinal("uinteger_max");
        var uBigIntMax = reader.GetOrdinal("ubigint_max");

        // Each column read as its own .NET type.
        reader.GetFieldValue<sbyte>(tinyIntMin).Should().Be(sbyte.MinValue);
        reader.GetFieldValue<sbyte>(tinyIntMax).Should().Be(sbyte.MaxValue);
        reader.GetInt16(smallIntMin).Should().Be(short.MinValue);
        reader.GetInt16(smallIntMax).Should().Be(short.MaxValue);
        reader.GetInt32(integerMin).Should().Be(int.MinValue);
        reader.GetInt32(integerMax).Should().Be(int.MaxValue);
        reader.GetInt64(bigIntMin).Should().Be(long.MinValue);
        reader.GetInt64(bigIntMax).Should().Be(long.MaxValue);
        reader.GetByte(uTinyIntMax).Should().Be(byte.MaxValue);
        reader.GetFieldValue<ushort>(uSmallIntMax).Should().Be(ushort.MaxValue);
        reader.GetFieldValue<uint>(uIntegerMax).Should().Be(uint.MaxValue);
        reader.GetFieldValue<ulong>(uBigIntMax).Should().Be(ulong.MaxValue);

        // One step wider holds the value; one step narrower does not.
        reader.GetInt64(integerMin).Should().Be(int.MinValue);
        reader.GetInt64(uIntegerMax).Should().Be(uint.MaxValue);
        reader.GetFieldValue<BigInteger>(uBigIntMax).Should().Be(new BigInteger(ulong.MaxValue));
        reader.Invoking(r => r.GetInt16(integerMin)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt32(bigIntMax)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt32(uIntegerMax)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(uBigIntMax)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<sbyte>(uTinyIntMax)).Should().Throw<InvalidCastException>();
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

        // Read as an integer, a fraction is rounded to the nearest even number: 1.5 and 2.5 both give 2.
        reader.GetInt32(0).Should().Be(2);
        reader.GetInt32(1).Should().Be(2);
        reader.GetInt32(2).Should().Be(2);
        reader.GetInt64(1).Should().Be(2);
    }

    [Fact]
    public void FloatReadAsDoubleIsAPlainWidening()
    {
        Command.CommandText = """
            SELECT 0.1::FLOAT AS tenth,
                   'NaN'::FLOAT AS nan,
                   'Infinity'::FLOAT AS inf,
                   '-Infinity'::FLOAT AS neg_inf,
                   '-0.0'::FLOAT AS neg_zero,
                   [0.1, 0.7]::FLOAT[] AS items
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var tenth = reader.GetOrdinal("tenth");
        var nan = reader.GetOrdinal("nan");
        var inf = reader.GetOrdinal("inf");
        var negInf = reader.GetOrdinal("neg_inf");
        var negZero = reader.GetOrdinal("neg_zero");
        var items = reader.GetOrdinal("items");

        // The double nearest to the float 0.1, which is not the double 0.1.
        reader.GetDouble(tenth).Should().Be((double)0.1f).And.Be(0.10000000149011612).And.NotBe(0.1);
        reader.GetFieldValue<double>(tenth).Should().Be((double)0.1f);
        reader.GetFieldValue<double?>(tenth).Should().Be((double)0.1f);

        double.IsNaN(reader.GetDouble(nan)).Should().BeTrue();
        double.IsNaN(reader.GetFieldValue<double?>(nan).Value).Should().BeTrue();
        reader.GetDouble(inf).Should().Be(double.PositiveInfinity);
        reader.GetFieldValue<double?>(inf).Should().Be(double.PositiveInfinity);
        reader.GetDouble(negInf).Should().Be(double.NegativeInfinity);
        reader.GetFieldValue<double?>(negInf).Should().Be(double.NegativeInfinity);

        // Negative zero equals zero, so the sign is checked on its own.
        reader.GetDouble(negZero).Should().Be(0d);
        double.IsNegative(reader.GetDouble(negZero)).Should().BeTrue();
        double.IsNegative(reader.GetFieldValue<double?>(negZero).Value).Should().BeTrue();

        reader.GetFieldValue<List<double>>(items).Should().Equal((double)0.1f, (double)0.7f);
    }

    // The C# cast, with no check: digits beyond what a float holds are lost, and a value too large for a float
    // reads as infinity. Npgsql does the same.
    [Fact]
    public void DoubleReadAsFloatIsAPlainNarrowing()
    {
        Command.CommandText = """
            SELECT 0.1::DOUBLE AS tenth,
                   123456789.123456789::DOUBLE AS many_digits,
                   1e300::DOUBLE AS too_large,
                   (-1e300)::DOUBLE AS too_large_negative,
                   1e-300::DOUBLE AS too_small,
                   'NaN'::DOUBLE AS nan,
                   'Infinity'::DOUBLE AS inf,
                   '-0.0'::DOUBLE AS neg_zero,
                   [0.1, 1e300]::DOUBLE[] AS items
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var tenth = reader.GetOrdinal("tenth");
        var manyDigits = reader.GetOrdinal("many_digits");
        var tooLarge = reader.GetOrdinal("too_large");
        var tooLargeNegative = reader.GetOrdinal("too_large_negative");
        var tooSmall = reader.GetOrdinal("too_small");
        var nan = reader.GetOrdinal("nan");
        var inf = reader.GetOrdinal("inf");
        var negZero = reader.GetOrdinal("neg_zero");
        var items = reader.GetOrdinal("items");

        reader.GetFloat(tenth).Should().Be(0.1f);
        reader.GetFieldValue<float>(tenth).Should().Be(0.1f);
        reader.GetFieldValue<float?>(tenth).Should().Be(0.1f);

        reader.GetFloat(manyDigits).Should().Be((float)123456789.123456789d).And.Be(123456792f);

        reader.GetFloat(tooLarge).Should().Be(float.PositiveInfinity);
        reader.GetFieldValue<float?>(tooLarge).Should().Be(float.PositiveInfinity);
        reader.GetFloat(tooLargeNegative).Should().Be(float.NegativeInfinity);
        reader.GetFloat(tooSmall).Should().Be(0f);

        float.IsNaN(reader.GetFloat(nan)).Should().BeTrue();
        reader.GetFloat(inf).Should().Be(float.PositiveInfinity);
        float.IsNegative(reader.GetFloat(negZero)).Should().BeTrue();

        reader.GetFieldValue<List<float>>(items).Should().Equal(0.1f, float.PositiveInfinity);
    }

    [Theory]
    [InlineData("FLOAT", "Single")]
    [InlineData("DOUBLE", "Double")]
    public void FloatingPointColumnReadAsInteger(string column, string clrName)
    {
        Command.CommandText = $"""
            SELECT (-0.5)::{column} AS minus_half,
                   (-1.5)::{column} AS minus_one_and_a_half,
                   (-2.5)::{column} AS minus_two_and_a_half,
                   0.5::{column} AS half,
                   3.5::{column} AS three_and_a_half,
                   (-3.5)::{column} AS minus_three_and_a_half,
                   'NaN'::{column} AS nan,
                   'Infinity'::{column} AS inf,
                   '-Infinity'::{column} AS neginf,
                   1e10::{column} AS big
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        // A half goes to the nearest even number, on both sides of zero.
        RoundsTo("minus_half", 0);
        RoundsTo("minus_one_and_a_half", -2);
        RoundsTo("minus_two_and_a_half", -2);
        RoundsTo("half", 0);
        RoundsTo("three_and_a_half", 4);
        RoundsTo("minus_three_and_a_half", -4);

        // NaN and the infinities are no integer, and 1e10 is too large for an int.
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

        void RoundsTo(string name, int expected)
        {
            var ordinal = reader.GetOrdinal(name);

            reader.GetInt32(ordinal).Should().Be(expected, $"column {name}");
            reader.GetInt64(ordinal).Should().Be(expected, $"column {name}");
            reader.GetFieldValue<int?>(ordinal).Should().Be(expected, $"column {name}");
            reader.GetFieldValue<short>(ordinal).Should().Be((short)expected, $"column {name}");
        }
    }

    // Each target type has its own range and its own rounding point, so a read as one integer type must not
    // go through another.
    [Fact]
    public void DoubleReadAsIntegerIsCheckedAgainstTheRangeOfTheRequestedType()
    {
        Command.CommandText = """
            SELECT (-0.5)::DOUBLE AS neghalf,
                   (-1.5)::DOUBLE AS negative,
                   1e19::DOUBLE AS huge,
                   255.5::DOUBLE AS above,
                   254.5::DOUBLE AS below,
                   'NaN'::DOUBLE AS nan
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var negHalf = reader.GetOrdinal("neghalf");
        var negative = reader.GetOrdinal("negative");
        var huge = reader.GetOrdinal("huge");
        var above = reader.GetOrdinal("above");
        var below = reader.GetOrdinal("below");

        // -0.5 rounds to zero, which every type holds.
        reader.GetFieldValue<ulong>(negHalf).Should().Be(0UL);
        reader.GetInt64(negHalf).Should().Be(0L);
        reader.GetByte(negHalf).Should().Be(0);

        // -1.5 rounds to -2.
        reader.GetInt64(negative).Should().Be(-2L);
        Refused<ulong>("negative");
        Refused<byte>("negative");

        // 1e19 is above the largest long and below the largest ulong.
        reader.GetFieldValue<ulong>(huge).Should().Be(10_000_000_000_000_000_000UL);
        reader.GetFieldValue<ulong?>(huge).Should().Be(10_000_000_000_000_000_000UL);
        Refused<long>("huge");
        Refused<byte>("huge");

        // 255.5 rounds to 256, one more than a byte holds. 254.5 rounds to 254.
        reader.GetFieldValue<ulong>(above).Should().Be(256UL);
        reader.GetInt64(above).Should().Be(256L);
        Refused<byte>("above");
        reader.GetByte(below).Should().Be(254);
        reader.GetFieldValue<byte?>(below).Should().Be((byte)254);
        reader.GetInt64(below).Should().Be(254L);

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
        Command.CommandText = $"""
            SELECT 1.5::{column} AS one_and_a_half,
                   0.1::{column} AS tenth,
                   (-123.456)::{column} AS negative,
                   1e20::{column} AS e20,
                   'NaN'::{column} AS nan,
                   'Infinity'::{column} AS inf,
                   '-Infinity'::{column} AS neginf,
                   1e30::{column} AS big
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var oneAndAHalf = reader.GetOrdinal("one_and_a_half");
        var tenth = reader.GetOrdinal("tenth");
        var negative = reader.GetOrdinal("negative");
        var e20 = reader.GetOrdinal("e20");

        reader.GetDecimal(oneAndAHalf).Should().Be(1.5m);
        reader.GetDecimal(tenth).Should().Be(0.1m);
        reader.GetDecimal(negative).Should().Be(-123.456m);
        reader.GetDecimal(e20).Should().Be(100000000000000000000m);
        reader.GetFieldValue<decimal>(tenth).Should().Be(0.1m);
        reader.GetFieldValue<decimal?>(tenth).Should().Be(0.1m);

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

        var negative = reader.GetOrdinal("negative");

        reader.Invoking(r => r.GetFieldValue<ulong>(negative)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from BigInteger to UInt64 in column negative");
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

        // -5 is below the range of a ulong, and 99999999999999999999 is above the range of a long.
        reader.Invoking(r => r.GetFieldValue<ulong>(1)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(2)).Should().Throw<InvalidCastException>();

        // Refused whatever the value.
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

    // The items of a list come from the same readers as a column of that type. The six tests below read lists.
    [Fact]
    public void ListOfIntegersReadsAsListsOfOtherNumericTypes()
    {
        Command.CommandText = "SELECT [1,2,3]::INTEGER[], [1,2]::BIGINT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<int>>(0).Should().Equal(1, 2, 3);
        reader.GetFieldValue<List<long>>(0).Should().Equal(1L, 2L, 3L);
        reader.GetFieldValue<List<short>>(0).Should().Equal((short)1, (short)2, (short)3);
        reader.GetFieldValue<List<double>>(0).Should().Equal(1d, 2d, 3d);
        reader.GetFieldValue<List<int?>>(0).Should().Equal(1, 2, 3);
        reader.GetValue(0).Should().BeOfType<List<int>>().Which.Should().Equal(1, 2, 3);

        reader.GetFieldValue<List<long>>(1).Should().Equal(1L, 2L);
        reader.GetFieldValue<List<int>>(1).Should().Equal(1, 2);
    }

    [Fact]
    public void ListWithANullItemReadsOnlyAsAListOfNullableItems()
    {
        Command.CommandText = "SELECT [1,NULL,3]::INTEGER[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<int?>>(0).Should().Equal(1, null, 3);
        reader.GetFieldValue<List<long?>>(0).Should().Equal(1L, null, 3L);
        reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ListWithAnItemThatDoesNotFitTheRequestedTypeThrows()
    {
        Command.CommandText = "SELECT [300,2]::INTEGER[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<long>>(0).Should().Equal(300L, 2L);
        reader.Invoking(r => r.GetFieldValue<List<byte>>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ListOfFloatingPointNumbersReadsAsFloatDoubleAndDecimal()
    {
        Command.CommandText = "SELECT [1.5,2.5]::DOUBLE[], [1.5,2.5]::FLOAT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<double>>(0).Should().Equal(1.5, 2.5);
        reader.GetFieldValue<List<decimal>>(0).Should().Equal(1.5m, 2.5m);
        reader.GetFieldValue<List<float>>(0).Should().Equal(1.5f, 2.5f);

        reader.GetFieldValue<List<float>>(1).Should().Equal(1.5f, 2.5f);
        reader.GetFieldValue<List<double>>(1).Should().Equal(1.5, 2.5);
        reader.GetFieldValue<List<decimal>>(1).Should().Equal(1.5m, 2.5m);
    }

    [Fact]
    public void ListsOfBooleansAndHugeInts()
    {
        Command.CommandText = "SELECT [true,false]::BOOLEAN[], [1,2]::HUGEINT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<bool>>(0).Should().Equal(true, false);
        reader.GetFieldValue<List<bool?>>(0).Should().Equal(true, false);

        reader.GetFieldValue<List<BigInteger>>(1).Should().Equal(new BigInteger(1), new BigInteger(2));
    }

    [Fact]
    public void NestedListOfIntegers()
    {
        Command.CommandText = "SELECT [[1,2],[3]]::INTEGER[][]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var nested = reader.GetFieldValue<List<List<int>>>(0);
        nested.Should().HaveCount(2);
        nested[0].Should().Equal(1, 2);
        nested[1].Should().Equal(3);
        reader.GetFieldValue<List<List<long>>>(0)[0].Should().Equal(1L, 2L);
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

        var doubles = reader.GetOrdinal("doubles");
        var floats = reader.GetOrdinal("floats");
        var fields = reader.GetOrdinal("fields");

        reader.GetFieldValue<Dictionary<int, decimal>>(doubles).Should().Equal(new Dictionary<int, decimal> { [1] = 1.5m });
        reader.GetFieldValue<Dictionary<int, float>>(doubles).Should().Equal(new Dictionary<int, float> { [1] = 1.5f });
        reader.GetFieldValue<Dictionary<int, decimal>>(floats).Should().Equal(new Dictionary<int, decimal> { [1] = 2.5m });
        reader.GetFieldValue<Dictionary<int, double>>(floats).Should().Equal(new Dictionary<int, double> { [1] = 2.5 });

        // The field 'single' is a FLOAT and the field 'double' is a DOUBLE.
        var asDecimals = reader.GetFieldValue<FloatingFields<decimal, decimal>>(fields);
        asDecimals.Single.Should().Be(2.5m);
        asDecimals.Double.Should().Be(1.5m);

        var swapped = reader.GetFieldValue<FloatingFields<double, float>>(fields);
        swapped.Single.Should().Be(2.5);
        swapped.Double.Should().Be(1.5f);
    }

    private class FloatingFields<TSingle, TDouble>
    {
        public TSingle Single { get; set; }
        public TDouble Double { get; set; }
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

    // The three tests below read 5000 rows. A chunk holds 2048 rows, so 5000 rows make the readers move to a
    // new chunk twice. Row i holds values made from i, and NULL where i is a multiple of the number in the
    // query. Every SQL expression has an exact result, which the test computes again from the row number.
    //
    // Each test runs with a materialized and with a streaming result. A materialized result can give the next
    // chunk the address of the previous one, and a reader that was not moved to the new chunk then still reads
    // the right values. A streaming result was not seen to do that.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IntegerAndFloatReadersKeepConvertingAcrossChunks(bool useStreamingMode)
    {
        const int rowCount = 5000;

        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = $"""
            SELECT i,
                   CASE WHEN i % 7 = 0 THEN NULL ELSE (i - 2500)::INTEGER END AS whole,
                   CASE WHEN i % 11 = 0 THEN NULL ELSE i::UBIGINT * 3600000000000000::UBIGINT END AS unsigned,
                   CASE WHEN i % 3 = 0 THEN NULL ELSE ((i - 2500)::DOUBLE / 8)::FLOAT END AS fraction
            FROM range(0, {rowCount}) t(i)
            ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var rowNumberColumn = reader.GetOrdinal("i");
        var wholeColumn = reader.GetOrdinal("whole");
        var unsignedColumn = reader.GetOrdinal("unsigned");
        var fractionColumn = reader.GetOrdinal("fraction");

        reader.GetFieldType(wholeColumn).Should().Be(typeof(int));
        reader.GetFieldType(unsignedColumn).Should().Be(typeof(ulong));
        reader.GetFieldType(fractionColumn).Should().Be(typeof(float));

        var row = 0;

        while (reader.Read())
        {
            reader.GetInt64(rowNumberColumn).Should().Be(row);

            int? whole = row % 7 == 0 ? null : row - 2500;
            decimal? unsigned = row % 11 == 0 ? null : (decimal)((ulong)row * 3600000000000000UL);
            double? fraction = row % 3 == 0 ? null : (double)((row - 2500) / 8f);

            // INTEGER read as long and as double.
            reader.GetFieldValue<long?>(wholeColumn).Should().Be(whole, $"row {row}");
            reader.GetFieldValue<double?>(wholeColumn).Should().Be(whole, $"row {row}");

            if (whole.HasValue)
            {
                reader.GetInt64(wholeColumn).Should().Be(whole.Value, $"row {row}");
                reader.GetDouble(wholeColumn).Should().Be(whole.Value, $"row {row}");
            }

            // UBIGINT read as decimal.
            reader.GetFieldValue<decimal?>(unsignedColumn).Should().Be(unsigned, $"row {row}");

            if (unsigned.HasValue)
            {
                reader.GetDecimal(unsignedColumn).Should().Be(unsigned.Value, $"row {row}");
            }

            // FLOAT read as double.
            reader.GetFieldValue<double?>(fractionColumn).Should().Be(fraction, $"row {row}");

            if (fraction.HasValue)
            {
                reader.GetDouble(fractionColumn).Should().Be(fraction.Value, $"row {row}");
            }

            row++;
        }

        row.Should().Be(rowCount);
    }

    // A DECIMAL(38,10) column, a list of it and a struct that holds it.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DecimalReadersKeepConvertingAcrossChunks(bool useStreamingMode)
    {
        const int rowCount = 5000;
        const decimal step = 1234567890123.0000000001m;

        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = $$"""
            SELECT i,
                   d,
                   CASE WHEN i % 13 = 0 THEN NULL ELSE [d, -d, NULL] END AS items,
                   {'d': d, 'i': i::INTEGER} AS fields
            FROM (SELECT i,
                         CASE WHEN i % 5 = 0 THEN NULL
                              ELSE ((i - 2500)::DECIMAL(5,0) * 1234567890123.0000000001::DECIMAL(23,10))::DECIMAL(38,10) END AS d
                  FROM range(0, {{rowCount}}) t(i))
            ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var rowNumberColumn = reader.GetOrdinal("i");
        var decimalColumn = reader.GetOrdinal("d");
        var itemsColumn = reader.GetOrdinal("items");
        var fieldsColumn = reader.GetOrdinal("fields");

        reader.GetFieldType(decimalColumn).Should().Be(typeof(decimal));

        var row = 0;

        while (reader.Read())
        {
            reader.GetInt64(rowNumberColumn).Should().Be(row);

            decimal? d = row % 5 == 0 ? null : (row - 2500) * step;

            reader.GetFieldValue<decimal?>(decimalColumn).Should().Be(d, $"row {row}");

            if (d.HasValue)
            {
                reader.GetDecimal(decimalColumn).Should().Be(d.Value, $"row {row}");
                reader.GetValue(decimalColumn).Should().Be(d.Value, $"row {row}");
            }

            // The list is NULL in some rows. In the others it holds d, -d and a NULL item, and d can be NULL too.
            if (row % 13 == 0)
            {
                reader.IsDBNull(itemsColumn).Should().BeTrue($"row {row}");
            }
            else
            {
                reader.GetFieldValue<List<decimal?>>(itemsColumn).Should().Equal([d, -d, null], $"row {row}");
            }

            var fields = reader.GetFieldValue<Dictionary<string, object>>(fieldsColumn);
            fields["d"].Should().Be(d, $"row {row}");
            fields["i"].Should().Be(row, $"row {row}");

            row++;
        }

        row.Should().Be(rowCount);
    }

    // The values are multiples of 10^22, which is more than a long holds.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void HugeIntAndVarIntReadersKeepConvertingAcrossChunks(bool useStreamingMode)
    {
        const int rowCount = 5000;

        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = $"""
            SELECT i,
                   CASE WHEN i % 17 = 0 THEN NULL ELSE (i - 2500)::HUGEINT * 10000000000000000000000::HUGEINT END AS huge,
                   CASE WHEN i % 19 = 0 THEN NULL ELSE ((i - 2500)::HUGEINT * 10000000000000000000000::HUGEINT)::VARINT END AS big
            FROM range(0, {rowCount}) t(i)
            ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var rowNumberColumn = reader.GetOrdinal("i");
        var hugeColumn = reader.GetOrdinal("huge");
        var bigColumn = reader.GetOrdinal("big");

        reader.GetFieldType(hugeColumn).Should().Be(typeof(BigInteger));
        reader.GetFieldType(bigColumn).Should().Be(typeof(BigInteger));

        var hugeStep = BigInteger.Pow(10, 22);
        var row = 0;

        while (reader.Read())
        {
            reader.GetInt64(rowNumberColumn).Should().Be(row);

            // Only row 2500 holds a value that fits a long: zero.
            BigInteger? huge = row % 17 == 0 ? null : (row - 2500) * hugeStep;
            BigInteger? big = row % 19 == 0 ? null : (row - 2500) * hugeStep;

            reader.GetFieldValue<BigInteger?>(hugeColumn).Should().Be(huge, $"row {row}");
            reader.GetFieldValue<BigInteger?>(bigColumn).Should().Be(big, $"row {row}");

            if (huge.HasValue)
            {
                reader.GetFieldValue<BigInteger>(hugeColumn).Should().Be(huge.Value, $"row {row}");
                reader.GetValue(hugeColumn).Should().Be(huge.Value, $"row {row}");
                reader.GetFieldValue<DuckDBHugeInt>(hugeColumn).ToBigInteger().Should().Be(huge.Value, $"row {row}");

                if (row == 2500)
                {
                    reader.GetInt64(hugeColumn).Should().Be(0);
                }
                else
                {
                    reader.Invoking(r => r.GetInt64(hugeColumn)).Should().Throw<InvalidCastException>($"row {row}");
                }
            }

            if (big.HasValue)
            {
                reader.GetFieldValue<BigInteger>(bigColumn).Should().Be(big.Value, $"row {row}");
                reader.GetValue(bigColumn).Should().Be(big.Value, $"row {row}");

                if (row == 2500)
                {
                    reader.GetInt64(bigColumn).Should().Be(0);
                }
            }

            row++;
        }

        row.Should().Be(rowCount);
    }

    // The helpers of ReadNumericColumnAsEveryNumericType.

    // Every value used there is a whole number that float, double and decimal hold exactly.
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

        // float, double, decimal and BigInteger hold every value the test uses.
        return true;
    }
}
