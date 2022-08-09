using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.DateTimeTests;

public class DateTests
{
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    [InlineData(1, 1, 1)]
    public void QueryScalarTest(int year, int mon, int day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT DATE '{year}-{mon}-{day}';";

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var dateOnly = (DateTime) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be(mon);
        dateOnly.Day.Should().Be(day);
        dateOnly.Hour.Should().Be(DateTime.MinValue.Hour);
        dateOnly.Minute.Should().Be(DateTime.MinValue.Minute);
        dateOnly.Second.Should().Be(DateTime.MinValue.Second);
        dateOnly.Minute.Should().Be(DateTime.MinValue.Millisecond);
    }
    
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    [InlineData(1, 1, 1)]
    public void BindParamTest(int year, int mon, int day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day);
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?::DATE;";
        cmd.Parameters.Add(new DuckDBParameter(expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var dateOnly = (DateTime) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be(mon);
        dateOnly.Day.Should().Be(day);
        dateOnly.Hour.Should().Be(DateTime.MinValue.Hour);
        dateOnly.Minute.Should().Be(DateTime.MinValue.Minute);
        dateOnly.Second.Should().Be(DateTime.MinValue.Second);
        dateOnly.Minute.Should().Be(DateTime.MinValue.Millisecond);
        
        dateOnly.Should().Be(expectedValue);
    }
    
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    [InlineData(1, 1, 1)]
    public void BindAndInsert(int year, int mon, int day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day);
        
        using var cmd = connection.CreateCommand();

        try
        {
            cmd.CommandText = "CREATE TABLE date_test (d DATE);";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "INSERT INTO date_test (d) VALUES (?);";
            cmd.Parameters.Add(new DuckDBParameter(expectedValue));
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM date_test;";
            cmd.Parameters.Clear();
            var scalar = cmd.ExecuteScalar();

            scalar.Should().BeOfType<DateTime>();

            var dateOnly = (DateTime) scalar;

            dateOnly.Year.Should().Be(year);
            dateOnly.Month.Should().Be(mon);
            dateOnly.Day.Should().Be(day);
            dateOnly.Hour.Should().Be(DateTime.MinValue.Hour);
            dateOnly.Minute.Should().Be(DateTime.MinValue.Minute);
            dateOnly.Second.Should().Be(DateTime.MinValue.Second);
            dateOnly.Minute.Should().Be(DateTime.MinValue.Millisecond);

            dateOnly.Should().Be(expectedValue);
        }
        finally
        {
            cmd.CommandText = "DROP TABLE date_test;";
            cmd.ExecuteNonQuery();
        }
    }
}