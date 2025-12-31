namespace DuckDB.NET.Test.Parameters;

public class ExplicitCastTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void CastWithFunction()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT strptime($date_as_text, '%Y-%m-%d %H:%M:%S') AS example;";
        command.Parameters.Add(new DuckDBParameter("date_as_text", "2023-04-02 01:01:00"));
        var scalar = command.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();
    }

    [Fact]
    public void CastWithExplicitCastToTimestamp()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $date_as_text::timestamp AS example;";
        command.Parameters.Add(new DuckDBParameter("date_as_text", "2023-04-02 01:01:00"));
        var scalar = command.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();
    }

    [Fact]
    public void CastWithExplicitCastFromInt()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $my_num::varchar AS example;";
        command.Parameters.Add(new DuckDBParameter("my_num", 42));
        var scalar = command.ExecuteScalar();

        scalar.Should().BeOfType<string>();
        scalar.Should().Be("42");
    }

    [Fact]
    public void CastWithExplicitCastToInt()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $my_num::int AS example;";
        command.Parameters.Add(new DuckDBParameter("my_num", "42"));
        var scalar = command.ExecuteScalar();

        scalar.Should().BeOfType<int>();
        scalar.Should().Be(42);
    }

    [Fact]
    public void CastWithExplicitCastToIntWrong()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $my_num::int AS example;";
        command.Parameters.Add(new DuckDBParameter("my_num", "Giorgi"));

        command.Invoking(c => c.ExecuteScalar())
            .Should().Throw<DuckDBException>().WithMessage("*Conversion Error*");
    }
}