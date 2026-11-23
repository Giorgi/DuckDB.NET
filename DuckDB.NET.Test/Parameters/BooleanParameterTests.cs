namespace DuckDB.NET.Test.Parameters;

public class BooleanParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    [Trait("V2", "Ready")]
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