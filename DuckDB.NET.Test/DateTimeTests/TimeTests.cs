using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.DateTimeTests;

public class TimeTests
{
    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    [InlineData(0, 0, 0, 0)]
    public void QueryScalarTest(int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT TIME '{hour}:{minute}:{second}.{millisecond:000000}';";

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var timeOnly = (DateTime) scalar;
        
        timeOnly.Year.Should().Be(DateTime.MinValue.Year);
        timeOnly.Month.Should().Be(DateTime.MinValue.Month);
        timeOnly.Day.Should().Be(DateTime.MinValue.Day);
        timeOnly.Hour.Should().Be(hour);
        timeOnly.Minute.Should().Be(minute);
        timeOnly.Second.Should().Be(second);
        timeOnly.Millisecond.Should().Be(millisecond);
    }
    
    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    [InlineData(0, 0, 0, 0)]
    public void BindParamTest(int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second, millisecond);
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?::TIME;";
        cmd.Parameters.Add(new DuckDBParameter(expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var timeOnly = (DateTime) scalar;
        
        timeOnly.Year.Should().Be(DateTime.MinValue.Year);
        timeOnly.Month.Should().Be(DateTime.MinValue.Month);
        timeOnly.Day.Should().Be(DateTime.MinValue.Day);
        timeOnly.Hour.Should().Be(hour);
        timeOnly.Minute.Should().Be(minute);
        timeOnly.Second.Should().Be(second);
        timeOnly.Millisecond.Should().Be(millisecond);
        
        timeOnly.Should().Be(expectedValue);
    }
    
    [Theory]
    [InlineData(12, 15, 17, 350)]
    [InlineData(12, 17, 15, 450)]
    [InlineData(18, 15, 17, 125)]
    [InlineData(0, 0, 0, 0)]
    public void BindAndInsert(int hour, int minute, int second, int millisecond)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
            hour, minute, second, millisecond);
        
        using var cmd = connection.CreateCommand();

        try
        {
            cmd.CommandText = "CREATE TABLE time_test (t TIME);";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO time_test (t) VALUES (?);";
            cmd.Parameters.Add(new DuckDBParameter(expectedValue));
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM time_test;";
            cmd.Parameters.Clear();
            var scalar = cmd.ExecuteScalar();

            scalar.Should().BeOfType<DateTime>();

            var timeOnly = (DateTime) scalar;

            timeOnly.Year.Should().Be(DateTime.MinValue.Year);
            timeOnly.Month.Should().Be(DateTime.MinValue.Month);
            timeOnly.Day.Should().Be(DateTime.MinValue.Day);
            timeOnly.Hour.Should().Be(hour);
            timeOnly.Minute.Should().Be(minute);
            timeOnly.Second.Should().Be(second);
            timeOnly.Millisecond.Should().Be(millisecond);
            
            timeOnly.Should().Be(expectedValue);
        }
        finally
        {
            cmd.CommandText = "DROP TABLE time_test;";
            cmd.ExecuteNonQuery();
        }
    }
}