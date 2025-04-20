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

        Command.CommandText = "SELECT ?::TIME;";
        Command.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<TimeOnly>();

        var timeOnly = (TimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Minute.Should().Be((byte)minute);
        timeOnly.Second.Should().Be((byte)second);
        timeOnly.Ticks.Should().Be(expectedValue.Ticks);

        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(expectedValue));

        var time = (TimeOnly)Command.ExecuteScalar();
        time.Should().Be(TimeOnly.FromTimeSpan(expectedValue.TimeOfDay));
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

        using (var reader = Command.ExecuteReader())
        {
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
        }

        Command.CommandText = "DROP TABLE TimeOnlyTestTable;";
        Command.ExecuteNonQuery();
    }

    [Theory]
    [InlineData(12, 15, 17, 350_000, 0, 0)]
    [InlineData(12, 17, 15, 450_000, -2, 0)]
    [InlineData(18, 15, 17, 125_000, -4, 30)]
    [InlineData(12, 15, 17, 350_300, 0, 30)]
    [InlineData(12, 17, 15, 450_500, 2, 45)]
    [InlineData(18, 15, 17, 125_700, 4, 30)]
    public void QueryTimeTzScalarTest(int hour, int minute, int second, int microsecond, int offsetHours, int offsetMinutes)
    {
        Command.CommandText = $"SELECT TIMETZ '{hour}:{minute}:{second}.{microsecond:000000}{offsetHours:00+##;00-##;}:{offsetMinutes:00}';";

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<DateTimeOffset>();

        var dateTimeOffset = (DateTimeOffset)scalar;

        dateTimeOffset.Hour.Should().Be((byte)hour);
        dateTimeOffset.Minute.Should().Be((byte)minute);
        dateTimeOffset.Second.Should().Be((byte)second);
        dateTimeOffset.Ticks.Should().Be(new TimeOnly(hour, minute, second).Add(TimeSpan.FromTicks(microsecond * 10)).Ticks);

        dateTimeOffset.Offset.Should().Be(new TimeSpan(offsetHours, offsetHours >= 0 ? offsetMinutes : -offsetMinutes, 0));
    }

    [Theory]
    [InlineData(12, 15, 17, 350_000, 0, 0)]
    [InlineData(12, 17, 15, 450_000, -2, 0)]
    [InlineData(18, 15, 17, 125_000, -4, 30)]
    [InlineData(12, 15, 17, 350_300, 0, 30)]
    [InlineData(12, 17, 15, 450_500, 2, 45)]
    [InlineData(18, 15, 17, 125_700, 4, 30)]
    public void QueryTimeTzReaderTest(int hour, int minute, int second, int microsecond, int offsetHours, int offsetMinutes)
    {
        Command.CommandText = $"SELECT TIMETZ '{hour}:{minute}:{second}.{microsecond:000000}{offsetHours:00+##;00-##;}:{offsetMinutes:00}';";

        var dataReader = Command.ExecuteReader();
        dataReader.Read();

        var dateTimeOffset = dataReader.GetFieldValue<DateTimeOffset>(0);
        dataReader.Dispose();

        dateTimeOffset.Hour.Should().Be((byte)hour);
        dateTimeOffset.Minute.Should().Be((byte)minute);
        dateTimeOffset.Second.Should().Be((byte)second);
        dateTimeOffset.Ticks.Should().Be(new TimeOnly(hour, minute, second).Add(TimeSpan.FromTicks(microsecond * 10)).Ticks);

        var timeSpan = new TimeSpan(offsetHours, offsetHours >= 0 ? offsetMinutes : -offsetMinutes, 0);
        dateTimeOffset.Offset.Should().Be(timeSpan);

        Command.CommandText = "SELECT ?::TIMETZ";
        Command.Parameters.Add(new DuckDBParameter(dateTimeOffset));

        using var reader = Command.ExecuteReader();
        reader.Read();

        var fieldValue = reader.GetFieldValue<DateTimeOffset>(0);
        fieldValue.Offset.Should().Be(timeSpan);
    }

    [Theory]
    [InlineData(12, 15, 17, 350_000)]
    [InlineData(12, 17, 15, 450_000)]
    [InlineData(18, 15, 17, 125_000)]
    [InlineData(12, 15, 17, 350_300)]
    [InlineData(12, 17, 15, 450_500)]
    [InlineData(18, 15, 17, 125_700)]
    public void BindTimeOnly(int hour, int minute, int second, int microsecond)
    {
        var expectedValue = new TimeOnly(hour, minute, second,0).Add(TimeSpan.FromMicroseconds(microsecond));

        Command.CommandText = "SELECT ?::TIME;";
        Command.Parameters.Add(new DuckDBParameter(expectedValue));

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<TimeOnly>();

        var timeOnly = (TimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Minute.Should().Be((byte)minute);
        timeOnly.Second.Should().Be((byte)second);
        timeOnly.Ticks.Should().Be(expectedValue.Ticks);
    }
}