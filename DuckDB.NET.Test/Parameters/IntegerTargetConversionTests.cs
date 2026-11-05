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
    private static DuckDBType SentType(object value, Type parameterType, DuckDBType parameterDuckDBType)
    {
        using var logicalType = parameterType.GetLogicalType();
        using var duckDBValue = value.ToDuckDBValue(logicalType, parameterDuckDBType, DuckDBTypeMap.GetDbTypeForValue(value));

        // The value owns its type: mark the handle invalid so that it is not destroyed a second time.
        var valueType = NativeMethods.Value.DuckDBGetValueType(duckDBValue);
        try
        {
            return NativeMethods.LogicalType.DuckDBGetTypeId(valueType);
        }
        finally
        {
            valueType.SetHandleAsInvalid();
        }
    }

    // An integer value or an enum that fits is converted in .NET, so DuckDB gets the parameter's own type and does not
    // pay for a cast on INSERT. Anything else goes as its own type.
    [Fact]
    public void ValueThatFitsIsSentAsTheTypeOfTheParameter()
    {
        foreach (var value in IntegerValues)
        {
            SentType(value, typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.Integer, value.GetType().Name);
            SentType(value, typeof(Int128), DuckDBType.HugeInt).Should().Be(DuckDBType.HugeInt, value.GetType().Name);
            SentType(value, typeof(UInt128), DuckDBType.UnsignedHugeInt).Should().Be(DuckDBType.UnsignedHugeInt, value.GetType().Name);
        }

        SentType(DayOfWeek.Friday, typeof(byte), DuckDBType.UnsignedTinyInt).Should().Be(DuckDBType.UnsignedTinyInt);

        SentType(300, typeof(sbyte), DuckDBType.TinyInt).Should().Be(DuckDBType.Integer);
        SentType(LongEnum.Large, typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.BigInt);
        SentType(2.5m, typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.Decimal);
        SentType("42", typeof(int), DuckDBType.Integer).Should().Be(DuckDBType.Varchar);
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

        // Beyond 128 bits the value goes as a BIGNUM, and DuckDB 2.0.0 casts that to its low bits instead of refusing
        // it (duckdb/duckdb#26242), so 2^128 is not checked.
        if (max + 1 < BigInteger.Pow(2, 128))
        {
            InsertFails(columnType, max + 1);
        }
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

    // These values are not sent as text any more; before, a BigInteger went as text in the current culture, and
    // "−5" (U+2212) was refused. The text fallback itself is pinned by the next test.
    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void NegativeValuesAreNotSentAsCultureText(string culture)
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);

            foreach (var column in new[] { "TINYINT", "INTEGER", "BIGINT", "HUGEINT" })
            {
                Insert(column, new BigInteger(-5)).Should().Be("-5", $"BigInteger into {column}");
                Insert(column, -5).Should().Be("-5", $"int into {column}");
                Insert(column, -2.5m).Should().Be("-3", $"decimal into {column}");
            }
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    // A DbType without a creator sends the value as text, formatted without the current culture.
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
            Command.Parameters.Add(new DuckDBParameter(-1.5d) { DbType = DbType.AnsiString });
            Command.ExecuteNonQuery();

            Command.CommandText = "SELECT v::VARCHAR FROM TextFallbackTests;";
            Command.Parameters.Clear();
            Command.ExecuteScalar().Should().Be("-1.50");

            Command.CommandText = "SELECT $1;";
            Command.Parameters.Add(new DuckDBParameter(-1.5m) { DbType = DbType.Decimal });
            Command.ExecuteScalar().Should().Be("-1.5");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
