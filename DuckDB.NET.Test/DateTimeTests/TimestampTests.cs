using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.DateTimeTests;

public class TimestampTests
{
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125)]
    [InlineData(1, 1, 1, 0, 0, 0, 0)]
    public void QueryScalarTest(int year, int mon, int day, int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day, hour, minute, second, millisecond);
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TIMESTAMP '{year}-{mon}-{day} {hour}:{minute}:{second}.{millisecond:000000}';";
        
        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var receivedTime = (DateTime) scalar;

        receivedTime.Year.Should().Be(year);
        receivedTime.Month.Should().Be(mon);
        receivedTime.Day.Should().Be(day);
        receivedTime.Hour.Should().Be(hour);
        receivedTime.Minute.Should().Be(minute);
        receivedTime.Second.Should().Be(second);
        receivedTime.Millisecond.Should().Be(millisecond);

        receivedTime.Should().Be(expectedValue);
    }
    
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125)]
    [InlineData(1, 1, 1, 0, 0, 0, 0)]
    public void BindTest(int year, int mon, int day, int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day, hour, minute, second, millisecond);
        
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
        receivedTime.Millisecond.Should().Be(millisecond);

        receivedTime.Should().Be(expectedValue);
    }
    
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125)]
    [InlineData(1, 1, 1, 0, 0, 0, 0)]
    public void BindAndInsert(int year, int mon, int day, int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day, hour, minute, second, millisecond);
        
        using var cmd = connection.CreateCommand();

        try
        {
            cmd.CommandText = "CREATE TABLE timestamp_test (d TIMESTAMP);";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO timestamp_test (d) VALUES (?);";
            cmd.Parameters.Add(new DuckDBParameter(expectedValue));
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM timestamp_test;";
            cmd.Parameters.Clear();
            var scalar = cmd.ExecuteScalar();

            scalar.Should().BeOfType<DateTime>();

            var receivedTime = (DateTime) scalar;

            receivedTime.Year.Should().Be(year);
            receivedTime.Month.Should().Be(mon);
            receivedTime.Day.Should().Be(day);
            receivedTime.Hour.Should().Be(hour);
            receivedTime.Minute.Should().Be(minute);
            receivedTime.Second.Should().Be(second);
            receivedTime.Millisecond.Should().Be(millisecond);

            receivedTime.Should().Be(expectedValue);
        }
        finally
        {
            cmd.CommandText = "DROP TABLE timestamp_test;";
            cmd.ExecuteNonQuery();
        }
    }
}