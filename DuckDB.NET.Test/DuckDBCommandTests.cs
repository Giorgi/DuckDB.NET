using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBCommandTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SetCommandText()
    {
        var cmd = new DuckDbCommand("Select 1");
        cmd.CommandText.Should().Be("Select 1");
    }

    [Fact]
    public void SetCommandTextAndConnection()
    {
        var cmd = new DuckDbCommand("Select 1", Connection);

        cmd.CommandText.Should().Be("Select 1");
        cmd.Connection.Should().Be(Connection);
    }
}