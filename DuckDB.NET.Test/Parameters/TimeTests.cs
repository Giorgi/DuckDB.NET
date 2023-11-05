using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class TimeTests
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
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TIME '{hour}:{minute}:{second}.{microsecond:000000}';";

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBTimeOnly>();

        var timeOnly = (DuckDBTimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Min.Should().Be((byte)minute);
        timeOnly.Sec.Should().Be((byte)second);
        timeOnly.Microsecond.Should().Be(microsecond);

        var dateTime = timeOnly.ToDateTime();
        dateTime.Year.Should().Be(DateTime.MinValue.Year);
        dateTime.Month.Should().Be(DateTime.MinValue.Month);
        dateTime.Day.Should().Be(DateTime.MinValue.Day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(microsecond / 1000);

        var convertedValue = (DateTime)timeOnly;
        convertedValue.Should().Be(dateTime);
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
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second).AddTicks(microsecond * 10);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?;";
        cmd.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBTimeOnly>();

        var timeOnly = (DuckDBTimeOnly)scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Min.Should().Be((byte)minute);
        timeOnly.Sec.Should().Be((byte)second);
        timeOnly.Microsecond.Should().Be(microsecond);

        var dateTime = timeOnly.ToDateTime();
        dateTime.Year.Should().Be(DateTime.MinValue.Year);
        dateTime.Month.Should().Be(DateTime.MinValue.Month);
        dateTime.Day.Should().Be(DateTime.MinValue.Day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(microsecond / 1000);

        var convertedValue = (DateTime)timeOnly;
        convertedValue.Should().Be(dateTime);
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
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second).AddTicks(microsecond * 10);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE TimeOnlyTestTable (a INTEGER, b TIME);";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO TimeOnlyTestTable (a, b) VALUES (42, ?);";
        cmd.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));
        cmd.ExecuteNonQuery();

        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT * FROM TimeOnlyTestTable LIMIT 1;";

        var reader = cmd.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(DuckDBTimeOnly));

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

        cmd.CommandText = "DROP TABLE TimeOnlyTestTable;";
        cmd.ExecuteNonQuery();
    }
}