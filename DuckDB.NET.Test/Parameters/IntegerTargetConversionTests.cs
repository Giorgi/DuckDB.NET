using DuckDB.NET.Data.Execution;
using System.Globalization;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.PreparedStatement;

namespace DuckDB.NET.Test.Parameters;

// A parameter whose type DuckDB reports as an integer type: an integer value or an enum that fits is converted to it,
// and any other value goes as its own type for DuckDB to cast, as it would cast a SQL literal.
public class IntegerTargetConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private enum ByteEnum : byte { Large = 200 }

    private enum LongEnum : long { Large = long.MaxValue }

    public static TheoryData<string> IntegerColumns =>
    [
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    ];

    public static TheoryData<object> IntegerValues =>
    [
        (sbyte)42, (byte)42, (short)42, (ushort)42, 42, 42u, 42L, 42ul, (Int128)42, (UInt128)42, new BigInteger(42),
    ];

    private string Insert(string columnType, object value)
    {
        Command.CommandText = $"CREATE OR REPLACE TABLE IntegerTargetTests (v {columnType});";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO IntegerTargetTests VALUES ($1);";
        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(value));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT v::VARCHAR FROM IntegerTargetTests;";
        Command.Parameters.Clear();
        return (string)Command.ExecuteScalar();
    }

    private void InsertFails(string columnType, object value)
    {
        FluentActions.Invoking(() => Insert(columnType, value)).Should().Throw<DuckDBException>($"{value} ({value.GetType().Name}) into {columnType}");
    }

    private string Where(string columnType, object value)
    {
        Command.CommandText = $"CREATE OR REPLACE TABLE IntegerTargetWhereTests (v {columnType}); INSERT INTO IntegerTargetWhereTests VALUES (1), (2);";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT coalesce(string_agg(v::VARCHAR, ','), 'none') FROM IntegerTargetWhereTests WHERE v = $1;";
        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(value));
        return (string)Command.ExecuteScalar();
    }

    // The DuckDB type of the value the converter creates for a parameter of the given type.
    private DuckDBType SentType(object value, Type parameterType, DuckDBType parameterDuckDBType)
    {
        return Helpers.V2ConvertedValue.Convert(Connection, value, factory => DuckDBV2Types.ForClrType(factory, parameterType),
                                                parameterDuckDBType, DuckDBTypeMap.GetDbTypeForValue(value)).Type;
    }

    // The ten integer parameter types, which are also the ten integer value types.
    private static readonly (Type Type, DuckDBType DuckDBType, object Min, object Max)[] Targets =
    [
        (typeof(sbyte), DuckDBType.TinyInt, sbyte.MinValue, sbyte.MaxValue),
        (typeof(short), DuckDBType.SmallInt, short.MinValue, short.MaxValue),
        (typeof(int), DuckDBType.Integer, int.MinValue, int.MaxValue),
        (typeof(long), DuckDBType.BigInt, long.MinValue, long.MaxValue),
        (typeof(Int128), DuckDBType.HugeInt, Int128.MinValue, Int128.MaxValue),
        (typeof(byte), DuckDBType.UnsignedTinyInt, byte.MinValue, byte.MaxValue),
        (typeof(ushort), DuckDBType.UnsignedSmallInt, ushort.MinValue, ushort.MaxValue),
        (typeof(uint), DuckDBType.UnsignedInteger, uint.MinValue, uint.MaxValue),
        (typeof(ulong), DuckDBType.UnsignedBigInt, ulong.MinValue, ulong.MaxValue),
        (typeof(UInt128), DuckDBType.UnsignedHugeInt, UInt128.MinValue, UInt128.MaxValue),
    ];

    public static TheoryData<Type, DuckDBType> IntegerTargets
    {
        get
        {
            var data = new TheoryData<Type, DuckDBType>();
            foreach (var target in Targets)
            {
                data.Add(target.Type, target.DuckDBType);
            }

            return data;
        }
    }

    private static BigInteger AsBigInteger(object value) => value switch
    {
        BigInteger big => big,
        Int128 int128 => int128,
        UInt128 uint128 => uint128,
        ulong uint64 => uint64,
        _ => Convert.ToInt64(value, CultureInfo.InvariantCulture),
    };

    // An integer value or an enum that fits is converted in .NET, so DuckDB gets the parameter's own type.
    [Theory]
    [MemberData(nameof(IntegerTargets))]
    public void ValueThatFitsIsSentAsTheTypeOfTheParameter(Type parameterType, DuckDBType parameterDuckDBType)
    {
        foreach (var value in IntegerValues)
        {
            SentType(value, parameterType, parameterDuckDBType).Should().Be(parameterDuckDBType, value.GetType().Name);
        }

        SentType(DayOfWeek.Friday, parameterType, parameterDuckDBType).Should().Be(parameterDuckDBType);
    }

    // Anything else goes as its own type.
    [Fact]
    public void ValueThatDoesNotFitIsSentAsItsOwnType()
    {
        SentType(300, typeof(sbyte), DuckDBType.TinyInt).Should().Be(DuckDBType.Integer);
        SentType(LongEnum.Large, typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.BigInt);
        SentType(2.5m, typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.Decimal);
        SentType("42", typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.Varchar);
    }

    // The limits of every integer type, one beyond them and the powers of two at the type boundaries, against every
    // integer parameter type: sent as the parameter's type exactly when the value is in its range.
    [Fact]
    public void ValueIsSentAsTheTypeOfTheParameterExactlyWhenItFits()
    {
        var values = new List<object>();
        foreach (var target in Targets)
        {
            values.AddRange([target.Min, target.Max, AsBigInteger(target.Min) - 1, AsBigInteger(target.Max) + 1]);
        }

        foreach (var exponent in new[] { 63, 64, 127, 128 })
        {
            values.AddRange([BigInteger.Pow(2, exponent), -BigInteger.Pow(2, exponent)]);
        }

        foreach (var target in Targets)
        {
            foreach (var value in values)
            {
                var number = AsBigInteger(value);
                var fits = number >= AsBigInteger(target.Min) && number <= AsBigInteger(target.Max);

                var ownType = value is BigInteger
                    ? number >= AsBigInteger(Int128.MinValue) && number <= AsBigInteger(Int128.MaxValue) ? DuckDBType.HugeInt
                      : number >= 0 && number <= AsBigInteger(UInt128.MaxValue) ? DuckDBType.UnsignedHugeInt : DuckDBType.VarInt
                    : Targets.Single(t => t.Type == value.GetType()).DuckDBType;

                SentType(value, target.Type, target.DuckDBType).Should().Be(fits ? target.DuckDBType : ownType,
                    $"{value} ({value.GetType().Name}) for {target.DuckDBType}");
            }
        }
    }

    // Some function arguments take only the exact type, so a long is narrowed to the INTEGER that lpad reports. A
    // value that is not an integer goes as its own type, and such an argument does not bind with it.
    [Fact]
    public void FunctionArgumentTakesAnIntegerThatFits()
    {
        Command.CommandText = "SELECT lpad('2', $1, '0');";
        Command.Parameters.Add(new DuckDBParameter(3L));
        Command.ExecuteScalar().Should().Be("002");

        Command.CommandText = "SELECT substring('abcdef', $1, 2);";
        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(3.0d));
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*Binder Error*");
    }

    [Theory]
    [MemberData(nameof(IntegerColumns))]
    public void EveryIntegerTypeIntoEveryIntegerColumn(string columnType)
    {
        foreach (var value in IntegerValues)
        {
            Insert(columnType, value).Should().Be("42", $"{value.GetType().Name} into {columnType}");
        }
    }

    [Theory]
    [InlineData("TINYINT", "-128", "127")]
    [InlineData("SMALLINT", "-32768", "32767")]
    [InlineData("INTEGER", "-2147483648", "2147483647")]
    [InlineData("BIGINT", "-9223372036854775808", "9223372036854775807")]
    [InlineData("HUGEINT", "-170141183460469231731687303715884105728", "170141183460469231731687303715884105727")]
    [InlineData("UTINYINT", "0", "255")]
    [InlineData("USMALLINT", "0", "65535")]
    [InlineData("UINTEGER", "0", "4294967295")]
    [InlineData("UBIGINT", "0", "18446744073709551615")]
    [InlineData("UHUGEINT", "0", "340282366920938463463374607431768211455")]
    public void TheLimitsOfEachTypeFitAndOneBeyondIsRefused(string columnType, string minimum, string maximum)
    {
        var min = BigInteger.Parse(minimum);
        var max = BigInteger.Parse(maximum);

        Insert(columnType, min).Should().Be(minimum);
        Insert(columnType, max).Should().Be(maximum);
        InsertFails(columnType, min - 1);
        InsertFails(columnType, max + 1);
    }

    // Beyond 128 bits a BigInteger goes as a BIGNUM. DuckDB 2.0.0 cast a BIGNUM to an integer type by its low bits, so
    // 2^128 + 1 was stored as 1; DuckDB 2.0.1 refuses it.
    [Fact]
    public void ValuesBeyond128BitsAreRefused()
    {
        var two128 = BigInteger.Pow(2, 128);

        foreach (var columnType in new[] { "INTEGER", "BIGINT", "HUGEINT", "UHUGEINT" })
        {
            InsertFails(columnType, two128);
            InsertFails(columnType, two128 + 5);
            InsertFails(columnType, -(two128 + 5));
        }

        Where("INTEGER", two128 + 1).Should().Be("none");

        Command.CommandText = "CREATE OR REPLACE TABLE IntegerTargetListTests (v INTEGER[]);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO IntegerTargetListTests VALUES ($1);";
        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(new List<BigInteger> { 1, two128 + 5 }));
        Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<DuckDBException>().WithMessage("*could not convert every item to Integer*");

        Command.CommandText = "CREATE OR REPLACE TABLE IntegerTargetTests (v INTEGER);";
        Command.Parameters.Clear();
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO IntegerTargetTests VALUES ($1);";
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.VarNumeric, Value = 1e40 });
        Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<DuckDBException>().WithMessage("*BIGNUM*out of range*");
    }

    [Fact]
    public void ValuesThatDoNotFitAreRefusedByDuckDB()
    {
        InsertFails("TINYINT", 300);
        InsertFails("UTINYINT", -1);
        InsertFails("UINTEGER", -1L);
        InsertFails("SMALLINT", Int128.MaxValue);
        InsertFails("UBIGINT", new BigInteger(ulong.MaxValue) + 1);
        InsertFails("BIGINT", ulong.MaxValue);
        InsertFails("UBIGINT", long.MinValue);
        InsertFails("HUGEINT", UInt128.MaxValue);
        InsertFails("BIGINT", Int128.MinValue);

        // An enum that does not fit goes as its number, not its name.
        FluentActions.Invoking(() => Insert("INTEGER", LongEnum.Large)).Should().Throw<DuckDBException>()
                     .WithMessage("*out of range*").Which.Message.Should().NotContain("Large");
    }

    [Fact]
    public void ValueThatDoesNotFitIsComparedAsItsOwnType()
    {
        Where("TINYINT", 300).Should().Be("none");
        Where("UTINYINT", -1).Should().Be("none");
        Where("TINYINT", LongEnum.Large).Should().Be("none");
        Where("TINYINT", 2L).Should().Be("2");
    }

    // A list item is converted with the type of the list's items, by the same rules.
    [Fact]
    public void ListItemsFollowTheSameRules()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE IntegerTargetListTests (v INTEGER[]);";
        Command.ExecuteNonQuery();

        foreach (var (value, expected) in new (object, string)[]
                 {
                     (new List<long> { 7, 8 }, "[7, 8]"), (new List<decimal> { 2.5m }, "[3]"), (new List<double> { 2.5, 3.5 }, "[2, 4]"),
                     (new List<string> { "7" }, "[7]"), (new List<bool> { true }, "[1]"), (new List<DayOfWeek> { DayOfWeek.Friday }, "[5]"),
                 })
        {
            Command.CommandText = "INSERT INTO IntegerTargetListTests VALUES ($1) RETURNING v::VARCHAR;";
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(value));
            Command.ExecuteScalar().Should().Be(expected, value.GetType().Name);
        }
    }

    [Fact]
    public void FractionsAreCastByDuckDB()
    {
        Insert("INTEGER", 1.5d).Should().Be("2");
        Insert("INTEGER", 2.5d).Should().Be("2", "DuckDB rounds a DOUBLE half to even");
        Insert("INTEGER", -2.5d).Should().Be("-2");
        Insert("INTEGER", 1.5f).Should().Be("2");
        Insert("INTEGER", 2.5m).Should().Be("3", "DuckDB rounds a DECIMAL half away from zero");
        Insert("INTEGER", -2.5m).Should().Be("-3");
        Insert("TINYINT", 2.5m).Should().Be("3");
        Insert("HUGEINT", 2.5m).Should().Be("3");
        InsertFails("INTEGER", 1e20d);
        InsertFails("UINTEGER", -2.5m);
    }

    [Fact]
    public void FractionInAComparisonIsNotRounded()
    {
        Where("INTEGER", 1.5d).Should().Be("none");
        Where("INTEGER", 1.5m).Should().Be("none");
        Where("BIGINT", 2.0d).Should().Be("2");
        Where("BIGINT", 2.0m).Should().Be("2");
    }

    [Fact]
    public void StringCharAndBooleanInAComparisonAreCastByDuckDB()
    {
        Where("INTEGER", "2").Should().Be("2");
        Where("INTEGER", " 2 ").Should().Be("2");
        Where("INTEGER", "2.0").Should().Be("2");
        Where("INTEGER", '2').Should().Be("2");
        Where("INTEGER", true).Should().Be("1");
        Where("INTEGER", false).Should().Be("none");
    }

    [Fact]
    public void StringsAreParsedByDuckDB()
    {
        Insert("INTEGER", "42").Should().Be("42");
        Insert("INTEGER", " 42 ").Should().Be("42");
        Insert("BIGINT", "-5").Should().Be("-5");
        Insert("INTEGER", "1.5").Should().Be("2");
        InsertFails("INTEGER", "x");
        InsertFails("INTEGER", "1,5");
    }

    [Fact]
    public void BooleanIsCastByDuckDB()
    {
        Insert("INTEGER", true).Should().Be("1");
        Insert("TINYINT", false).Should().Be("0");
    }

    [Fact]
    public void CharIsSentAsText()
    {
        Insert("INTEGER", '5').Should().Be("5");
        Insert("UTINYINT", '7').Should().Be("7");
        InsertFails("INTEGER", 'A');
    }

    [Fact]
    public void EnumIsSentAsItsNumber()
    {
        Insert("INTEGER", DayOfWeek.Friday).Should().Be("5");
        Insert("UTINYINT", ByteEnum.Large).Should().Be("200");
        Insert("HUGEINT", LongEnum.Large).Should().Be(long.MaxValue.ToString(CultureInfo.InvariantCulture));
        Where("INTEGER", (DayOfWeek)2).Should().Be("2");
    }

    [Fact]
    public void OtherTypesAreRefusedByDuckDB()
    {
        InsertFails("INTEGER", new DateTime(2026, 11, 5));
        InsertFails("BIGINT", Guid.NewGuid());
    }

    // A BigInteger is not sent as text any more; before, it went as text in the current culture, and sv-SE's "−5"
    // (U+2212) was refused. The text fallback itself is pinned by the next test.
    [Fact]
    public void NegativeBigIntegerIsNotSentAsCultureText()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("sv-SE");
            CultureInfo.CurrentCulture.NumberFormat.NegativeSign.Should().Be("−", "the test needs the ICU data of sv-SE");

            foreach (var column in new[] { "TINYINT", "INTEGER", "BIGINT", "HUGEINT" })
            {
                Insert(column, new BigInteger(-5)).Should().Be("-5", $"BigInteger into {column}");
            }
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    // A value with no DuckDB type of its own (a Half) goes as text, formatted without the current culture.
    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void TextFallbackDoesNotDependOnTheCurrentCulture(string culture)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);

            Command.CommandText = "CREATE OR REPLACE TABLE TextFallbackTests (v DECIMAL(10,2));";
            Command.ExecuteNonQuery();

            Command.CommandText = "INSERT INTO TextFallbackTests VALUES ($1);";
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter((Half)(-1.5)));
            Command.ExecuteNonQuery();

            Command.CommandText = "SELECT v::VARCHAR FROM TextFallbackTests;";
            Command.Parameters.Clear();
            Command.ExecuteScalar().Should().Be("-1.50");

            Command.CommandText = "SELECT $1;";
            Command.Parameters.Add(new DuckDBParameter((Half)(-1.5)));
            Command.ExecuteScalar().Should().Be("-1.5");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
