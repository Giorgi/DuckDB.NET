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
    
    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void InsertAndQueryTest(int year, byte mon, byte day)
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE DateOnlyTestTable (a INTEGER, b DATE);";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO DateOnlyTestTable (a, b) VALUES (42, ?);";
        cmd.Parameters.Add(new DuckDBParameter(new DuckDBDateOnly {Year = year, Month = mon, Day = day}));
        cmd.ExecuteNonQuery();
        
        cmd.Parameters.Clear();
        cmd.CommandText = "SELECT * FROM DateOnlyTestTable LIMIT 1;";

        var reader = cmd.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(DuckDBDateOnly));

        var dateOnly = reader.GetDateOnly(1);

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be(mon);
        dateOnly.Day.Should().Be(day);

        var dateTime = dateOnly.ToDateTime();
        dateTime.Year.Should().Be(year);
        dateTime.Month.Should().Be(mon);
        dateTime.Day.Should().Be(day);
        dateTime.Hour.Should().Be(0);
        dateTime.Minute.Should().Be(0);
        dateTime.Second.Should().Be(0);

        var convertedValue = (DateTime) dateOnly;
        convertedValue.Should().Be(dateTime);

        cmd.CommandText = "DROP TABLE DateOnlyTestTable;";
        cmd.ExecuteNonQuery();
    }
}