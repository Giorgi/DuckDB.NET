using System.Globalization;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class DecimalParameterTests
{
    [Fact]
    public void SimpleTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var values = new []{0m, decimal.Zero, decimal.MinValue, 
            decimal.MaxValue, decimal.MaxValue / 3, decimal.One, 
            decimal.One / 2, decimal.One / 3, decimal.MinusOne, 
            decimal.MinusOne / 2, decimal.MinusOne / 3};

        foreach (var value in values)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT '{value}';";
            command.ExecuteNonQuery();

            var scalar = command.ExecuteScalar();
            scalar.Should().Be(value.ToString(CultureInfo.InvariantCulture));

            var reader = command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetDecimal(0);
            receivedValue.Should().Be(value);
        }
    }

    [Fact]
    public void BindValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var values = new []{0m, decimal.Zero, decimal.MinValue, 
            decimal.MaxValue, decimal.MaxValue / 3, decimal.One, 
            decimal.One / 2, decimal.One / 3, decimal.MinusOne, 
            decimal.MinusOne / 2, decimal.MinusOne / 3};

        foreach (var value in values)
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT ?;";
            command.Parameters.Add(new DuckDBParameter(value));
            command.ExecuteNonQuery();

            var scalar = command.ExecuteScalar();
            scalar.Should().Be(value.ToString(CultureInfo.InvariantCulture));

            var reader = command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetDecimal(0);
            receivedValue.Should().Be(value);
        }
    }
}