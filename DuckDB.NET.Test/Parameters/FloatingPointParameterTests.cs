using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class FloatingPointParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void BindParameterWithoutTable_Double()
    {
        var value = Faker.Random.Double(-1_000_000, 1_000_000);
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        var result = Command.ExecuteScalar();
        result.Should().BeOfType<double>().Subject.Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_Float()
    {
        var value = Faker.Random.Float(-1_000_000, 1_000_000);
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        var result = Command.ExecuteScalar();
        result.Should().BeOfType<float>().Subject.Should().Be(value);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void BindParameterWithoutTable_DoubleSpecial(double value)
    {
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        var result = Command.ExecuteScalar();
        result.Should().BeOfType<double>();
        if (double.IsNaN(value))
        {
            double.IsNaN((double)result).Should().BeTrue();
        }
        else
        {
            result.Should().Be(value);
        }
    }

    [Theory]
    [InlineData(float.NaN)]
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public void BindParameterWithoutTable_FloatSpecial(float value)
    {
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        var result = Command.ExecuteScalar();
        result.Should().BeOfType<float>();
        if (float.IsNaN(value))
        {
            float.IsNaN((float)result).Should().BeTrue();
        }
        else
        {
            result.Should().Be(value);
        }
    }
}