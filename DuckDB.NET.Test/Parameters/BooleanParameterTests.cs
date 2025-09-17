using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class BooleanParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void BindParameterWithoutTable()
    {
        var value = Faker.Random.Bool();

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        var result = Command.ExecuteScalar();

        result.Should().BeOfType<bool>().Subject
              .Should().Be(value);
    }
}