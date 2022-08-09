using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class TimeTests
{
    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    public void QueryScalarTest(int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TIME '{hour}:{minute}:{second}.{millisecond:000000}';";

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBTimeOnly>();

        var timeOnly = (DuckDBTimeOnly) scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Min.Should().Be((byte)minute);
        timeOnly.Sec.Should().Be((byte)second);
        timeOnly.Msec.Should().Be(millisecond);

        var dateTime = timeOnly.ToDateTime();
        dateTime.Year.Should().Be(DateTime.MinValue.Year);
        dateTime.Month.Should().Be(DateTime.MinValue.Month);
        dateTime.Day.Should().Be(DateTime.MinValue.Day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(millisecond);

        var convertedValue = (DateTime) timeOnly;
        convertedValue.Should().Be(dateTime);
    }
    
    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    public void BindWithCastTest(int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second, millisecond);
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?;";
        cmd.Parameters.Add(new DuckDBParameter((DuckDBTimeOnly)expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBTimeOnly>();

        var timeOnly = (DuckDBTimeOnly) scalar;

        timeOnly.Hour.Should().Be((byte)hour);
        timeOnly.Min.Should().Be((byte)minute);
        timeOnly.Sec.Should().Be((byte)second);
        timeOnly.Msec.Should().Be(millisecond);

        var dateTime = timeOnly.ToDateTime();
        dateTime.Year.Should().Be(DateTime.MinValue.Year);
        dateTime.Month.Should().Be(DateTime.MinValue.Month);
        dateTime.Day.Should().Be(DateTime.MinValue.Day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(millisecond);

        var convertedValue = (DateTime) timeOnly;
        convertedValue.Should().Be(dateTime);
    }
}