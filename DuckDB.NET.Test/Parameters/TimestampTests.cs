using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class TimestampTests
{
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_500)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700)]
    public void QueryScalarTest(int year, int mon, int day, int hour, int minute, int second, int microsecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var expectedValue = new DateTime(year, mon, day, hour, minute, second).AddTicks(microsecond * 10);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TIMESTAMP '{year}-{mon}-{day} {hour}:{minute}:{second}.{microsecond:000000}';";
        
        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var receivedTime = (DateTime) scalar;

        receivedTime.Year.Should().Be(year);
        receivedTime.Month.Should().Be(mon);
        receivedTime.Day.Should().Be(day);
        receivedTime.Hour.Should().Be(hour);
        receivedTime.Minute.Should().Be(minute);
        receivedTime.Second.Should().Be(second);
        receivedTime.Millisecond.Should().Be(microsecond / 1000);

        receivedTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);
    }
    
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_500)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700)]
    public void BindTest(int year, int mon, int day, int hour, int minute, int second, int microsecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var expectedValue = new DateTime(year, mon, day, hour, minute, second).AddTicks(microsecond * 10);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?;";
        cmd.Parameters.Add(new DuckDBParameter(expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var receivedTime = (DateTime) scalar;

        receivedTime.Year.Should().Be(year);
        receivedTime.Month.Should().Be(mon);
        receivedTime.Day.Should().Be(day);
        receivedTime.Hour.Should().Be(hour);
        receivedTime.Minute.Should().Be(minute);
        receivedTime.Second.Should().Be(second);
        receivedTime.Millisecond.Should().Be(microsecond / 1000);

        receivedTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);
    }
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_500)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700)]
    public void InsertAndQueryTest(int year, int mon, int day, byte hour, byte minute, byte second, int microsecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var expectedValue = new DateTime(year, mon, day, hour, minute, second).AddTicks(microsecond * 10);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE TimestampTestTable (a INTEGER, b TIMESTAMP);";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO TimestampTestTable (a, b) VALUES (42, ?);";
        cmd.Parameters.Add(new DuckDBParameter(expectedValue));
        cmd.ExecuteNonQuery();
        
        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT * FROM TimestampTestTable LIMIT 1;";

        var reader = cmd.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(DateTime));

        var dateTime = reader.GetDateTime(1);

        dateTime.Year.Should().Be(year);
        dateTime.Month.Should().Be(mon);
        dateTime.Day.Should().Be(day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(microsecond / 1000);

        dateTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);

        var dateTimeNullable = reader.GetFieldValue<DateTime?>(1);
        dateTime = dateTimeNullable.Value;

        dateTime.Year.Should().Be(year);
        dateTime.Month.Should().Be(mon);
        dateTime.Day.Should().Be(day);
        dateTime.Hour.Should().Be(hour);
        dateTime.Minute.Should().Be(minute);
        dateTime.Second.Should().Be(second);
        dateTime.Millisecond.Should().Be(microsecond / 1000);

        dateTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);

        cmd.CommandText = "DROP TABLE TimestampTestTable;";
        cmd.ExecuteNonQuery();
    }
}