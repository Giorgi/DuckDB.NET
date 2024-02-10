using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class DateTests : DuckDBTestBase
{
    public DateTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void QueryScalarTest(int year, int mon, int day)
    {
        Command.CommandText = $"SELECT DATE '{year}-{mon}-{day}';";

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<DateOnly>();

        var dateOnly = (DateOnly) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be((byte)mon);
        dateOnly.Day.Should().Be((byte)day);
    }

    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void BindWithCastTest(int year, int mon, int day)
    {
        var expectedValue = new DateTime(year, mon, day);
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter((DuckDBDateOnly)expectedValue));

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<DateOnly>();

        var dateOnly = (DateOnly) scalar;

        dateOnly.Year.Should().Be(year);
        dateOnly.Month.Should().Be((byte)mon);
        dateOnly.Day.Should().Be((byte)day);
    }

    [Theory]
    [InlineData(1992, 09, 20)]
    [InlineData(2022, 05, 04)]
    [InlineData(2022, 04, 05)]
    public void InsertAndQueryTest(int year, byte mon, byte day)
    {
        Command.CommandText = "CREATE TABLE DateOnlyTestTable (a INTEGER, b DATE not null, nullableDateColumn Date);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO DateOnlyTestTable (a, b) VALUES (42, ?);";
        Command.Parameters.Add(new DuckDBParameter(new DuckDBDateOnly (year,mon,day)));
        Command.ExecuteNonQuery();
        
        Command.Parameters.Clear();
        Command.CommandText = "SELECT * FROM DateOnlyTestTable LIMIT 1;";

        var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(DateOnly));

        var dateOnly = reader.GetFieldValue<DuckDBDateOnly>(1);

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

        reader.GetFieldValue<DateOnly>(1).Should().Be(new DateOnly(year, mon, day));

        var convertedValue = (DateTime) dateOnly;
        convertedValue.Should().Be(dateTime);

        reader.GetFieldValue<DuckDBDateOnly?>(2).Should().BeNull();
        reader.Invoking(dataReader => dataReader.GetFieldValue<DuckDBDateOnly>(2)).Should().Throw<InvalidCastException>().Where(ex => ex.Message.Contains("nullableDateColumn"));

        Command.CommandText = "DROP TABLE DateOnlyTestTable;";
        Command.ExecuteNonQuery();
    }
}