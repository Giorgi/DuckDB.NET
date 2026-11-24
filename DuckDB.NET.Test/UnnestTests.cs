namespace DuckDB.NET.Test;

public class UnnestTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void UnnestStringList()
    {
        var names = new List<string> { "Bob", "Sam" };
        Command.CommandText = "SELECT unnest($names);";
        Command.Parameters.Add(new DuckDBParameter("names", names));

        using var reader = Command.ExecuteReader();

        var results = new List<string>();
        while (reader.Read())
        {
            results.Add(reader.GetString(0));
        }

        results.Should().BeEquivalentTo(names);
    }

    [Fact]
    public void UnnestStringListWithNull()
    {
        var names = new List<string> { "Bob", null, "Sam" };
        Command.CommandText = "SELECT unnest($names);";
        Command.Parameters.Add(new DuckDBParameter("names", names));

        using var reader = Command.ExecuteReader();

        var results = new List<string>();
        while (reader.Read())
        {
            results.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
        }

        results.Should().BeEquivalentTo(names);
    }

    [Fact]
    public void UnnestIntList()
    {
        var numbers = new List<int> { 1, 2, 3, 42 };
        Command.CommandText = "SELECT unnest($numbers);";
        Command.Parameters.Add(new DuckDBParameter("numbers", numbers));

        using var reader = Command.ExecuteReader();

        var results = new List<int>();
        while (reader.Read())
        {
            results.Add(reader.GetInt32(0));
        }

        results.Should().BeEquivalentTo(numbers);
    }

    [Fact]
    public void UnnestLongList()
    {
        var numbers = new List<long> { 1L, 2L, long.MaxValue };
        Command.CommandText = "SELECT unnest($numbers);";
        Command.Parameters.Add(new DuckDBParameter("numbers", numbers));

        using var reader = Command.ExecuteReader();

        var results = new List<long>();
        while (reader.Read())
        {
            results.Add(reader.GetInt64(0));
        }

        results.Should().BeEquivalentTo(numbers);
    }

    [Fact]
    public void UnnestDoubleList()
    {
        var numbers = new List<double> { 1.5, 2.5, 3.14 };
        Command.CommandText = "SELECT unnest($numbers);";
        Command.Parameters.Add(new DuckDBParameter("numbers", numbers));

        using var reader = Command.ExecuteReader();

        var results = new List<double>();
        while (reader.Read())
        {
            results.Add(reader.GetDouble(0));
        }

        results.Should().BeEquivalentTo(numbers);
    }

    [Fact]
    public void UnnestDecimalList()
    {
        var numbers = new List<decimal> { 1.1m, 2.22m, 3.333m };
        Command.CommandText = "SELECT unnest($numbers);";
        Command.Parameters.Add(new DuckDBParameter("numbers", numbers));

        using var reader = Command.ExecuteReader();

        var results = new List<decimal>();
        while (reader.Read())
        {
            results.Add(reader.GetDecimal(0));
        }

        results.Should().BeEquivalentTo(numbers);
    }

    [Fact]
    public void UnnestNestedIntList()
    {
        var nested = new List<List<int>> { new() { 1, 2 }, new() { 3, 4, 5 } };
        Command.CommandText = "SELECT unnest($nested);";
        Command.Parameters.Add(new DuckDBParameter("nested", nested));

        using var reader = Command.ExecuteReader();

        var results = new List<List<int>>();
        while (reader.Read())
        {
            results.Add(((IEnumerable<int>)reader.GetValue(0)).ToList());
        }

        results.Should().BeEquivalentTo(nested);
    }
}
