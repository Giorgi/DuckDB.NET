using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class DateTests
{
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void QueryScalarTest(int year, int mon, int day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT DATE '{year}-{mon}-{day}';";

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBDateOnly>();

        var dateOnly = (DuckDBDateOnly) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be((byte)mon);
        dateOnly.Day.Should().Be((byte)day);

        var dateTime = dateOnly.ToDateTime();
        dateTime.Year.Should().Be(year);
        dateTime.Month.Should().Be(mon);
        dateTime.Day.Should().Be(day);
        dateTime.Hour.Should().Be(0);
        dateTime.Minute.Should().Be(0);
        dateTime.Second.Should().Be(0);

        var convertedValue = (DateTime) dateOnly;
        convertedValue.Should().Be(dateTime);
    }
    
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void BindWithCastTest(int year, int mon, int day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        
        var expectedValue = new DateTime(year, mon, day);
        
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT ?;";
        cmd.Parameters.Add(new DuckDBParameter((DuckDBDateOnly)expectedValue));

        var scalar = cmd.ExecuteScalar();

        scalar.Should().BeOfType<DuckDBDateOnly>();

        var dateOnly = (DuckDBDateOnly) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be((byte)mon);
        dateOnly.Day.Should().Be((byte)day);

        var dateTime = dateOnly.ToDateTime();
        dateTime.Year.Should().Be(year);
        dateTime.Month.Should().Be(mon);
        dateTime.Day.Should().Be(day);
        dateTime.Hour.Should().Be(0);
        dateTime.Minute.Should().Be(0);
        dateTime.Second.Should().Be(0);

        var convertedValue = (DateTime) dateOnly;
        convertedValue.Should().Be(dateTime);
    }
}