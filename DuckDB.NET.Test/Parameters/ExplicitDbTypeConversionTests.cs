using System.Collections;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.Parameters;

// A DbType the caller assigns converts the value to its CLR type when nothing is lost, and refuses it otherwise; the
// rules for the parameter's type then apply to the converted value. Without an assigned DbType the type follows the
// value and converts nothing.
public class ExplicitDbTypeConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    public enum LargeEnum : long { Value = 1000 }

    // DuckDB reports no type for a parameter in a subquery's select list. A cast of the parameter itself would give
    // it the cast's type ("SELECT $1::VARCHAR" reports VARCHAR).
    private const string Untyped = "SELECT typeof(v), v::VARCHAR FROM (SELECT $1 AS v)";

    // The DbType, the value, then the type and text Untyped gives.
    public static TheoryData<DbType, object, string, string> Accepted => new()
    {
        { DbType.Int32, 5L, "INTEGER", "5" },
        { DbType.Int32, (Int128)5, "INTEGER", "5" },
        { DbType.Int32, new BigInteger(-5), "INTEGER", "-5" },
        { DbType.Int32, 2.0, "INTEGER", "2" },
        { DbType.Int32, -0.0, "INTEGER", "0" },
        { DbType.Int32, 2.0f, "INTEGER", "2" },
        { DbType.Int32, 2.00m, "INTEGER", "2" },
        { DbType.Int32, DayOfWeek.Tuesday, "INTEGER", "2" },
        { DbType.Int64, 5, "BIGINT", "5" },
        { DbType.Int16, (byte)5, "SMALLINT", "5" },
        { DbType.SByte, -5L, "TINYINT", "-5" },
        { DbType.Byte, 200, "UTINYINT", "200" },
        { DbType.UInt16, 5, "USMALLINT", "5" },
        { DbType.UInt32, 5L, "UINTEGER", "5" },
        { DbType.UInt64, 5, "UBIGINT", "5" },
        { DbType.UInt64, 1e19, "UBIGINT", "10000000000000000000" },
        { DbType.VarNumeric, 5, "HUGEINT", "5" },
        { DbType.VarNumeric, 1e30, "HUGEINT", "1000000000000000019884624838656" },
        { DbType.VarNumeric, 1e40, "BIGNUM", "10000000000000000303786028427003666890752" },
        { DbType.Single, 5, "FLOAT", "5.0" },
        { DbType.Single, 1.5, "FLOAT", "1.5" },
        { DbType.Double, 5L, "DOUBLE", "5.0" },
        { DbType.Double, 1.5f, "DOUBLE", "1.5" },
        { DbType.Double, 0.1m, "DOUBLE", "0.1" },
        { DbType.Decimal, 5, "DECIMAL(1,0)", "5" },
        { DbType.Decimal, 0.1, "DECIMAL(2,1)", "0.1" },
        { DbType.Decimal, 0.1f, "DECIMAL(2,1)", "0.1" },
        { DbType.Currency, 2.5, "DECIMAL(2,1)", "2.5" },
        { DbType.Decimal, (Int128)12345, "DECIMAL(5,0)", "12345" },
        { DbType.String, 'x', "VARCHAR", "x" },
        { DbType.AnsiString, "abc", "VARCHAR", "abc" },
        { DbType.StringFixedLength, 'y', "VARCHAR", "y" },
        { DbType.Boolean, true, "BOOLEAN", "true" },
        { DbType.Guid, new Guid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "UUID", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" },
        { DbType.Binary, new byte[] { 1, 2 }, "BLOB", "\\x01\\x02" },
        { DbType.Date, new DateTime(2026, 11, 19, 13, 45, 30), "DATE", "2026-11-19" },
        { DbType.Date, new DateOnly(2026, 11, 19), "DATE", "2026-11-19" },
        { DbType.Time, new TimeSpan(13, 45, 30), "TIME", "13:45:30" },
        { DbType.Time, new DateTime(2026, 11, 19, 13, 45, 30), "TIME", "13:45:30" },
        { DbType.Time, new TimeOnly(13, 45, 30), "TIME", "13:45:30" },
        { DbType.DateTime, new DateOnly(2026, 11, 19), "TIMESTAMP", "2026-11-19 00:00:00" },
        { DbType.DateTime2, new DateTime(2026, 11, 19, 13, 45, 30), "TIMESTAMP", "2026-11-19 13:45:30" },
        { DbType.DateTimeOffset, new DateTime(2026, 11, 19, 13, 45, 30), "TIMESTAMP WITH TIME ZONE", null },
        { DbType.Int64, -9.223372036854775808e18, "BIGINT", "-9223372036854775808" },     // -2^63, the smallest long
        { DbType.UInt64, 18446744073709549568.0, "UBIGINT", "18446744073709549568" },     // 2^64 - 2048, the largest double below 2^64
        { DbType.SByte, -128f, "TINYINT", "-128" },
        { DbType.Int32, (nint)5, "INTEGER", "5" },
        { DbType.UInt16, (nuint)5, "USMALLINT", "5" },
        { DbType.Double, (Half)1.5, "DOUBLE", "1.5" },
        { DbType.Int32, (Half)2, "INTEGER", "2" },
        { DbType.Date, new DuckDBDateOnly(2026, 11, 19), "DATE", "2026-11-19" },
        { DbType.Time, new DuckDBTimeOnly(13, 45, 30), "TIME", "13:45:30" },
        { DbType.DateTime, new DuckDBTimestamp(new DuckDBDateOnly(2026, 11, 19), new DuckDBTimeOnly(13, 45, 30)), "TIMESTAMP", "2026-11-19 13:45:30" },
        { DbType.Object, 5, "INTEGER", "5" },
        { DbType.Object, DayOfWeek.Tuesday, "VARCHAR", "Tuesday" },
    };

    [Theory]
    [MemberData(nameof(Accepted))]
    public void AcceptedWhereDuckDBReportsNoType(DbType dbType, object value, string type, string text)
    {
        Command.CommandText = Untyped;
        Command.Parameters.Add(new DuckDBParameter { DbType = dbType, Value = value });

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be(type);

        if (text != null)
        {
            reader.GetString(1).Should().Be(text);
        }
    }

    // The DbType, the value, then the message after "Cannot cast from ".
    public static TheoryData<DbType, object, string> Refused => new()
    {
        { DbType.Int32, 2.5, "Double to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part" },
        { DbType.Int32, 2.5m, "Decimal to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part" },
        { DbType.Int32, 3_000_000_000L, "Int64 to Int32 for parameter $1 (DbType.Int32): the value is out of range" },
        { DbType.Int32, 3e9, "Double to Int32 for parameter $1 (DbType.Int32): the value is out of range" },
        { DbType.Int32, double.NaN, "Double to Int32 for parameter $1 (DbType.Int32): the value is out of range" },
        { DbType.Byte, -1, "Int32 to Byte for parameter $1 (DbType.Byte): the value is out of range" },
        { DbType.Byte, LargeEnum.Value, "LargeEnum to Byte for parameter $1 (DbType.Byte): the value is out of range" },
        { DbType.UInt64, -1.0, "Double to UInt64 for parameter $1 (DbType.UInt64): the value is out of range" },
        { DbType.Int32, double.PositiveInfinity, "Double to Int32 for parameter $1 (DbType.Int32): the value is out of range" },
        { DbType.VarNumeric, double.NaN, "Double to BigInteger for parameter $1 (DbType.VarNumeric): the value is out of range" },
        { DbType.Double, DayOfWeek.Tuesday, "DayOfWeek to Double for parameter $1 (DbType.Double)" },
        { DbType.Int64, 9.223372036854775808e18, "Double to Int64 for parameter $1 (DbType.Int64): the value is out of range" },    // 2^63
        { DbType.UInt64, 18446744073709551616.0, "Double to UInt64 for parameter $1 (DbType.UInt64): the value is out of range" },  // 2^64
        { DbType.SByte, 128f, "Single to SByte for parameter $1 (DbType.SByte): the value is out of range" },
        { DbType.Decimal, 1e-30, "Double to Decimal for parameter $1 (DbType.Decimal): the value is out of range" },
        { DbType.Decimal, -1e-30f, "Single to Decimal for parameter $1 (DbType.Decimal): the value is out of range" },
        { DbType.Int32, (Half)2.5, "Half to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part" },
        { DbType.Int32, new HashSet<long> { 1 }, "HashSet<Int64> to Int32 for parameter $1 (DbType.Int32)" },
        { DbType.DateTime, new DuckDBDateOnly(2026, 11, 19), "DuckDBDateOnly to DateTime for parameter $1 (DbType.DateTime)" },
        { DbType.UInt64, 2e19, "Double to UInt64 for parameter $1 (DbType.UInt64): the value is out of range" },
        { DbType.Int32, "42", "String to Int32 for parameter $1 (DbType.Int32). Remove the DbType to let DuckDB convert the text, or parse the value first" },
        { DbType.Int32, true, "Boolean to Int32 for parameter $1 (DbType.Int32)" },
        { DbType.Int32, new DateTime(2026, 11, 19), "DateTime to Int32 for parameter $1 (DbType.Int32)" },
        { DbType.VarNumeric, 2.5, "Double to BigInteger for parameter $1 (DbType.VarNumeric): the value has a non-zero fractional part" },
        { DbType.Double, "1.5", "String to Double for parameter $1 (DbType.Double). Remove the DbType to let DuckDB convert the text, or parse the value first" },
        { DbType.Decimal, double.NaN, "Double to Decimal for parameter $1 (DbType.Decimal): the value is out of range" },
        { DbType.Decimal, 1e30, "Double to Decimal for parameter $1 (DbType.Decimal): the value is out of range" },
        { DbType.Decimal, BigInteger.Pow(10, 30), "BigInteger to Decimal for parameter $1 (DbType.Decimal): the value is out of range" },
        { DbType.Decimal, DayOfWeek.Tuesday, "DayOfWeek to Decimal for parameter $1 (DbType.Decimal)" },
        { DbType.String, 42, "Int32 to String for parameter $1 (DbType.String)" },
        { DbType.AnsiString, 1.5, "Double to String for parameter $1 (DbType.AnsiString)" },
        { DbType.Boolean, 1, "Int32 to Boolean for parameter $1 (DbType.Boolean)" },
        { DbType.Guid, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "String to Guid for parameter $1 (DbType.Guid). Remove the DbType to let DuckDB convert the text, or parse the value first" },
        { DbType.Binary, "ab", "String to Byte[] for parameter $1 (DbType.Binary). Remove the DbType to let DuckDB convert the text, or parse the value first" },
        { DbType.Date, "2026-11-19", "String to DateOnly for parameter $1 (DbType.Date). Remove the DbType to let DuckDB convert the text, or parse the value first" },
        { DbType.Date, new DateTimeOffset(2026, 11, 19, 0, 0, 0, TimeSpan.Zero), "DateTimeOffset to DateOnly for parameter $1 (DbType.Date)" },
        { DbType.Time, 5L, "Int64 to TimeOnly for parameter $1 (DbType.Time)" },
        { DbType.Time, TimeSpan.FromDays(1), "TimeSpan to TimeOnly for parameter $1 (DbType.Time): the value is out of range" },
        { DbType.DateTime, new DateTimeOffset(2026, 11, 19, 0, 0, 0, TimeSpan.Zero), "DateTimeOffset to DateTime for parameter $1 (DbType.DateTime)" },
        { DbType.DateTimeOffset, new DateOnly(2026, 11, 19), "DateOnly to DateTimeOffset for parameter $1 (DbType.DateTimeOffset)" },
    };

    [Theory]
    [MemberData(nameof(Refused))]
    public void RefusedWhereDuckDBReportsNoType(DbType dbType, object value, string message)
    {
        Command.CommandText = "SELECT $1";
        Command.Parameters.Add(new DuckDBParameter { DbType = dbType, Value = value });

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from " + message);
    }

    // A refused value is refused whatever the parameter's type: the DbType decides, not the statement.
    [Theory]
    [MemberData(nameof(Refused))]
    public void RefusedWhereDuckDBReportsAType(DbType dbType, object value, string message)
    {
        Command.CommandText = "SELECT $1 = 1";
        Command.Parameters.Add(new DuckDBParameter { DbType = dbType, Value = value });

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from " + message);
    }

    [Fact]
    public void TheMessageNamesANamedParameter()
    {
        Command.CommandText = "SELECT $id";
        Command.Parameters.Add(new DuckDBParameter("id", 2.5) { DbType = DbType.Int32 });

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from Double to Int32 for parameter \"id\" (DbType.Int32): the value has a non-zero fractional part");
    }

    [Fact]
    public void TheMessageNamesTheSecondParameter()
    {
        Command.CommandText = "SELECT $1, $2";
        Command.Parameters.Add(new DuckDBParameter(1));
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int32, Value = "2" });

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from String to Int32 for parameter $2 (DbType.Int32)*");
    }

    // After the DbType has converted the value, it is fitted to the column: a long with DbType.Int64 still goes into
    // an INTEGER column as an INTEGER, and an int with DbType.Int64 into a BIGINT one.
    [Fact]
    public void TheConvertedValueIsFittedToTheColumn()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeColumns (i INTEGER, b BIGINT, d DATE, t TIME, m DECIMAL(18,4), v VARCHAR)");

        Command.CommandText = "INSERT INTO ExplicitDbTypeColumns VALUES ($1, $2, $3, $4, $5, $6) " +
                              "RETURNING i::VARCHAR || ' ' || b::VARCHAR || ' ' || d::VARCHAR || ' ' || t::VARCHAR || ' ' || m::VARCHAR || ' ' || v";
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int64, Value = 5L });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int64, Value = 6 });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Date, Value = new DateTime(2026, 11, 19, 13, 45, 30) });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Time, Value = new TimeSpan(13, 45, 30) });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Decimal, Value = 0.1 });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.String, Value = 'x' });

        Command.ExecuteScalar().Should().Be("5 6 2026-11-19 13:45:30 0.1000 x");
    }

    // Without the DbType DuckDB rounds 2.5 to 2 for an INTEGER column; with it the value is refused.
    [Fact]
    public void AFractionIsRefusedForAnIntegerColumn()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeFraction (i INTEGER)");

        Command.CommandText = "INSERT INTO ExplicitDbTypeFraction VALUES ($1)";
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int32, Value = 2.5 });
        Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<InvalidCastException>();

        Command.Parameters[0].ResetDbType();
        Command.ExecuteNonQuery().Should().Be(1);

        Connection.Query<int>("SELECT i FROM ExplicitDbTypeFraction").Should().Equal(2);
    }

    // A DbType on a collection applies to each item.
    [Fact]
    public void TheDbTypeOfAListAppliesToItsItems()
    {
        Command.CommandText = Untyped;
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int32, Value = new List<long> { 1, 2 } });

        using (var reader = Command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetString(1).Should().Be("[1, 2]");
        }

        Command.Parameters[0].Value = new List<double> { 1, 2.5 };
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from Double to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part");
    }

    // An Int128 with DbType.Int32 that fits is an INTEGER, where it used to throw.
    [Fact]
    public void AnInt128WithAnIntegerDbTypeThatFits()
    {
        Command.CommandText = "SELECT $1";
        Command.Parameters.Add(new DuckDBParameter(DbType.Int32, (Int128)5));

        Command.ExecuteScalar().Should().Be(5);
    }

    // Items of a list with no DbType go as their own type, so DuckDB casts them to the list's item type.
    [Fact]
    public void AListOfInt128ForABooleanList()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeBooleans (b BOOLEAN[])");

        Command.CommandText = "INSERT INTO ExplicitDbTypeBooleans VALUES ($1) RETURNING b::VARCHAR";
        Command.Parameters.Add(new DuckDBParameter(new List<Int128> { 42, 0 }));

        Command.ExecuteScalar().Should().Be("[true, false]");
    }

    // SqlClient's convention gives a TimeSpan DbType.Time; DuckDB has no cast from TIME to INTERVAL, so for an
    // INTERVAL parameter the TimeSpan stays as it is, a day or more included.
    [Fact]
    public void ATimeSpanWithDbTypeTimeForAnInterval()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeIntervals (i INTERVAL, l INTERVAL[])");

        Command.CommandText = "INSERT INTO ExplicitDbTypeIntervals VALUES ($1, $2) RETURNING i::VARCHAR || ' ' || l::VARCHAR";
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, TimeSpan.FromHours(1)));
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, new List<TimeSpan> { TimeSpan.FromDays(2) }));
        Command.ExecuteScalar().Should().Be("01:00:00 [2 days]");

        Command.Parameters.Clear();
        Command.CommandText = "SELECT (TIMESTAMP '2026-01-01' + $1::INTERVAL)::VARCHAR";
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, TimeSpan.FromDays(2)));
        Command.ExecuteScalar().Should().Be("2026-01-03 00:00:00");
    }

    // Where DuckDB reports a type, a DateTime with DbType.DateTimeOffset is not made a DateTimeOffset, so a TIMESTAMP
    // keeps its clock time instead of having it moved by the session's time zone.
    [Fact]
    public void ADateTimeWithDbTypeDateTimeOffsetForATimestamp()
    {
        Connection.ExecuteNonQuery("SET TimeZone = 'Asia/Tbilisi'");
        try
        {
            Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeTimestamps (t TIMESTAMP, z TIMESTAMPTZ)");

            Command.CommandText = "INSERT INTO ExplicitDbTypeTimestamps VALUES ($1, $2) RETURNING t::VARCHAR || ' ' || z::VARCHAR";
            Command.Parameters.Add(new DuckDBParameter(DbType.DateTimeOffset, new DateTime(2026, 11, 19, 13, 45, 30)));
            Command.Parameters.Add(new DuckDBParameter(DbType.DateTimeOffset, new DateTime(2026, 11, 19, 13, 45, 30)));

            // The TIMESTAMPTZ takes the clock time as UTC, as it does without the DbType.
            Command.ExecuteScalar().Should().Be("2026-11-19 13:45:30 2026-11-19 17:45:30+04");

            Command.Parameters.Clear();
            // Where DuckDB reports no type, the DbType makes it a TIMESTAMPTZ, the clock time taken as UTC.
            Command.CommandText = "SELECT v::VARCHAR FROM (SELECT $1 AS v)";
            Command.Parameters.Add(new DuckDBParameter(DbType.DateTimeOffset, new DateTime(2026, 11, 19, 13, 45, 30)));
            Command.ExecuteScalar().Should().Be("2026-11-19 17:45:30+04");
        }
        finally
        {
            Connection.ExecuteNonQuery("RESET TimeZone");
        }
    }

    // A list DuckDB reports no type for has items of the DbType's type, as a single value with that DbType has.
    [Theory]
    [InlineData(DbType.Int32, "INTEGER[]", "[1, 2]")]
    [InlineData(DbType.Int64, "BIGINT[]", "[1, 2]")]
    [InlineData(DbType.Double, "DOUBLE[]", "[1.0, 2.0]")]
    [InlineData(DbType.Decimal, "BIGINT[]", "[1, 2]")]      // the element type stays (DECIMAL(38,18) could be too small)
    [InlineData(DbType.VarNumeric, "BIGINT[]", "[1, 2]")]
    public void AnUntypedListHasItemsOfTheDbTypesType(DbType dbType, string type, string text)
    {
        Command.CommandText = Untyped;
        Command.Parameters.Add(new DuckDBParameter(dbType, new List<long> { 1, 2 }));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be(type);
        reader.GetString(1).Should().Be(text);
    }

    // A nested list keeps its element type, and the DbType goes on to the items of the inner lists. A large value with
    // Decimal or VarNumeric keeps the element type too, which holds it, where DECIMAL(38,18) or HUGEINT would not.
    [Fact]
    public void AnUntypedListKeepsItsElementTypeWhenTheDbTypesTypeWouldNotHoldIt()
    {
        Command.CommandText = Untyped;
        var parameter = new DuckDBParameter(DbType.Int32, new List<List<long>> { new() { 1 }, new() { 2 } });
        Command.Parameters.Add(parameter);

        foreach (var (value, type, text) in new (object, string, string)[]
                 {
                     (new List<List<long>> { new() { 1 }, new() { 2 } }, "BIGINT[][]", "[[1], [2]]"),
                     (new List<double> { 1e25 }, "DOUBLE[]", "[1e+25]"),
                 })
        {
            parameter.DbType = value is List<double> ? DbType.Decimal : DbType.Int32;
            parameter.Value = value;

            using var reader = Command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(type);
            reader.GetString(1).Should().Be(text);
        }

        parameter.DbType = DbType.VarNumeric;
        parameter.Value = new List<UInt128> { UInt128.MaxValue };
        using (var reader = Command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("UHUGEINT[]");
            reader.GetString(1).Should().Be($"[{UInt128.MaxValue}]");
        }

        parameter.Value = new List<List<double>> { new() { 1.5 } };
        parameter.DbType = DbType.Int32;
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from Double to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part");
    }

    // With no element type to keep (an ArrayList, or object items), Decimal and VarNumeric give the list their types.
    [Theory]
    [InlineData(DbType.Decimal, "DECIMAL(38,18)[]")]
    [InlineData(DbType.VarNumeric, "HUGEINT[]")]
    [InlineData(DbType.Int32, "INTEGER[]")]
    public void AnUntypedListWithoutAnElementTypeTakesTheDbTypesType(DbType dbType, string type)
    {
        Command.CommandText = Untyped;
        var parameter = new DuckDBParameter(dbType, new ArrayList { 1, 2 });
        Command.Parameters.Add(parameter);

        foreach (var value in new ICollection[] { new ArrayList { 1, 2 }, new List<object> { 1, 2 } })
        {
            parameter.Value = value;

            using var reader = Command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(type);
        }
    }

    // A list item refused after others were created: the refusal comes through, and the created items are released.
    [Fact]
    public void AListItemRefusedPartWayThrough()
    {
        Command.CommandText = Untyped;
        Command.Parameters.Add(new DuckDBParameter(DbType.Int32, new List<object> { 1, 2, "x" }));

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from String to Int32 for parameter $1 (DbType.Int32)*");

        Command.Parameters[0].Value = new List<object> { 1, 2, 3 };
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(1).Should().Be("[1, 2, 3]");
    }

    // A TimeSpan out of a day for a TIME_NS parameter, with no DbType, goes as text for DuckDB to refuse, as for TIME.
    [Fact]
    public void ATimeSpanOutOfADayForATimeNsColumn()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeTimeNs (t TIME_NS)");

        Command.CommandText = "INSERT INTO ExplicitDbTypeTimeNs VALUES ($1)";
        Command.Parameters.Add(new DuckDBParameter(TimeSpan.FromHours(25)));
        Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<DuckDBException>();

        Command.Parameters[0].Value = TimeSpan.FromHours(23);
        Command.ExecuteNonQuery().Should().Be(1);
    }

    // A type nested in a generic class is generic but has no backtick in its name.
    [Fact]
    public void TheMessageNamesATypeNestedInAGenericClass()
    {
        Command.CommandText = "SELECT $1";
        Command.Parameters.Add(new DuckDBParameter(DbType.Int32, new List<int>().GetEnumerator()));

        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from Enumerator to Int32 for parameter $1 (DbType.Int32)");
    }

    [Fact]
    public void AnUntypedListOfTimeSpansWithDbTypeTimeIsAListOfTimes()
    {
        Command.CommandText = Untyped;
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, new List<TimeSpan> { TimeSpan.FromHours(1) }));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("TIME[]");
        reader.GetString(1).Should().Be("['01:00:00']");
    }

    // A typed list takes its items with the DbType applied to each.
    [Fact]
    public void ATypedListWithADbType()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeLists (i INTEGER[], d DATE[3])");

        Command.CommandText = "INSERT INTO ExplicitDbTypeLists VALUES ($1, $2) RETURNING i::VARCHAR || ' ' || d::VARCHAR";
        Command.Parameters.Add(new DuckDBParameter(DbType.Int32, new List<double> { 1, 2 }));
        Command.Parameters.Add(new DuckDBParameter(DbType.Date, new[] { new DateTime(2026, 11, 19, 13, 0, 0), new DateTime(2026, 11, 20), new DateTime(2026, 11, 21) }));
        Command.ExecuteScalar().Should().Be("[1, 2] [2026-11-19, 2026-11-20, 2026-11-21]");

        Command.Parameters[0].Value = new List<double> { 1.5 };
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<InvalidCastException>()
            .WithMessage("Cannot cast from Double to Int32 for parameter $1 (DbType.Int32): the value has a non-zero fractional part");
    }

    // A byte[] for a list of UTINYINT is a list of its bytes, with DbType.Byte applied to each and DbType.Binary to none.
    [Theory]
    [InlineData(DbType.Byte)]
    [InlineData(DbType.Binary)]
    [InlineData(DbType.Object)]
    public void AByteArrayForAListOfBytes(DbType dbType)
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypeBytes (b UTINYINT[])");

        Command.CommandText = "INSERT INTO ExplicitDbTypeBytes VALUES ($1) RETURNING b::VARCHAR";
        Command.Parameters.Add(new DuckDBParameter(dbType, new byte[] { 1, 2 }));

        Command.ExecuteScalar().Should().Be("[1, 2]");
    }

    // A refused value on a prepared command leaves the command usable, and a value that converts then works.
    [Fact]
    public void APreparedCommandRefusesAndThenTakesAValue()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE ExplicitDbTypePrepared (i INTEGER)");

        Command.CommandText = "INSERT INTO ExplicitDbTypePrepared VALUES ($1)";
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Int32, Value = 2.5 });
        Command.Prepare();

        Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<InvalidCastException>();

        Command.Parameters[0].Value = 3.0;
        Command.ExecuteNonQuery().Should().Be(1);
        Command.Parameters[0].Value = 4L;
        Command.ExecuteNonQuery().Should().Be(1);

        Connection.Query<int>("SELECT i FROM ExplicitDbTypePrepared ORDER BY rowid").Should().Equal(3, 4);
    }

    // A null is a NULL whatever the DbType.
    [Theory]
    [InlineData(DbType.Int32)]
    [InlineData(DbType.Date)]
    [InlineData(DbType.String)]
    public void ANullWithADbType(DbType dbType)
    {
        Command.CommandText = "SELECT $1 IS NULL, $2 IS NULL";
        Command.Parameters.Add(new DuckDBParameter { DbType = dbType, Value = null });
        Command.Parameters.Add(new DuckDBParameter { DbType = dbType, Value = DBNull.Value });

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetBoolean(0).Should().BeTrue();
        reader.GetBoolean(1).Should().BeTrue();
    }

    // A DbType set before the value stays, so the value is converted (DbType.Time makes a TimeSpan a time of day).
    [Fact]
    public void ADbTypeSetBeforeTheValueConvertsIt()
    {
        Command.CommandText = "SELECT typeof($1)";
        var parameter = Command.CreateParameter();
        parameter.DbType = DbType.Time;
        parameter.Value = new TimeSpan(13, 45, 30);
        Command.Parameters.Add(parameter);

        Command.ExecuteScalar().Should().Be("TIME");

        parameter.ResetDbType();
        Command.ExecuteScalar().Should().Be("INTERVAL");
    }
}
