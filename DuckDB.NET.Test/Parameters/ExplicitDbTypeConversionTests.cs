using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.Parameters;

// A DbType the caller assigns converts the value to its CLR type when nothing is lost, and refuses it otherwise; the
// rules for the parameter's type then apply to the converted value. Without an assigned DbType the type follows the
// value and converts nothing.
public class ExplicitDbTypeConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    public enum LargeEnum : long { Value = 1000 }

    // The DbType, the value, then what "SELECT typeof($1), $1::VARCHAR" gives where DuckDB reports no type.
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
        // DuckDB gives typeof() of a string bound to a parameter with no type as UNKNOWN: a string literal.
        { DbType.String, 'x', "UNKNOWN", "x" },
        { DbType.AnsiString, "abc", "UNKNOWN", "abc" },
        { DbType.StringFixedLength, 'y', "UNKNOWN", "y" },
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
        { DbType.Object, 5, "INTEGER", "5" },
        { DbType.Object, DayOfWeek.Tuesday, "UNKNOWN", "Tuesday" },
    };

    [Theory]
    [MemberData(nameof(Accepted))]
    public void AcceptedWhereDuckDBReportsNoType(DbType dbType, object value, string type, string text)
    {
        Command.CommandText = "SELECT typeof($1), $1::VARCHAR";
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
        Command.CommandText = "SELECT typeof($1), $1::VARCHAR";
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
