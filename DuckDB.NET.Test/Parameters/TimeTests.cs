using System;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class TimeTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    [InlineData(12, 15, 17, 350_000)]
    [InlineData(12, 17, 15, 450_000)]
    [InlineData(18, 15, 17, 125_000)]
    [InlineData(12, 15, 17, 350_300)]
    [InlineData(12, 17, 15, 450_500)]
    [InlineData(18, 15, 17, 125_700)]
    public void QueryScalarTest(int hour, int minute, int second, int microsecond)
    {
        Command.CommandText = $"SELECT TIME '{hour}:{minute}:{second}.{microsecond:000000}';";

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<TimeOnly>();

        var timeOnly = (TimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Minute.Should().Be((byte)minute);
        timeOnly.Second.Should().Be((byte)second);
        timeOnly.Ticks.Should().Be(new TimeOnly(hour, minute, second).Add(TimeSpan.FromTicks(microsecond * 10)).Ticks);
    }

    [Theory]
    [InlineData(12, 15, 17, 350_000)]
    [InlineData(12, 17, 15, 450_000)]
    [InlineData(18, 15, 17, 125_000)]
    [InlineData(12, 15, 17, 350_300)]
    [InlineData(12, 17, 15, 450_500)]
    [InlineData(18, 15, 17, 125_700)]
    public void BindWithCastTest(int hour, int minute, int second, int microsecond)
    {
        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second).AddTicks(microsecond * 10);

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<TimeOnly>();

        var timeOnly = (TimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Minute.Should().Be((byte)minute);
        timeOnly.Second.Should().Be((byte)second);
        timeOnly.Ticks.Should().Be(expectedValue.Ticks);
    }

    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    [InlineData(12, 15, 17, 350_300)]
    [InlineData(12, 17, 15, 450_500)]
    [InlineData(18, 15, 17, 125_700)]
    public void InsertAndQueryTest(byte hour, byte minute, byte second, int microsecond)
    {
        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second).AddTicks(microsecond * 10);

        Command.CommandText = "CREATE TABLE TimeOnlyTestTable (a INTEGER, b TIME);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO TimeOnlyTestTable (a, b) VALUES (42, ?);";
        Command.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));
        Command.ExecuteNonQuery();

        Command.Parameters.Clear();
        Command.CommandText = "SELECT * FROM TimeOnlyTestTable LIMIT 1;";

        var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(TimeOnly));

        var duckDBTimeOnly = reader.GetFieldValue<DuckDBTimeOnly>(1);

        duckDBTimeOnly.Hour.Should().Be(hour);
        duckDBTimeOnly.Min.Should().Be(minute);
        duckDBTimeOnly.Sec.Should().Be(second);
        duckDBTimeOnly.Microsecond.Should().Be(microsecond);

        var dateTime = duckDBTimeOnly.ToDateTime();
        dateTime.Year.Should().Be(DateTime.MinValue.Year);
        dateTime.Month.Should().Be(DateTime.MinValue.Month);
        dateTime.Day.Should().Be(DateTime.MinValue.Day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(microsecond / 1000);

        var convertedValue = (DateTime)duckDBTimeOnly;
        convertedValue.Should().Be(dateTime);

        var timeOnly = reader.GetFieldValue<TimeOnly>(1);
        timeOnly.Should().Be(new TimeOnly(hour, minute, second).Add(TimeSpan.FromTicks(microsecond * 10)));

        Command.CommandText = "DROP TABLE TimeOnlyTestTable;";
        Command.ExecuteNonQuery();
    }
}