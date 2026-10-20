namespace DuckDB.NET.Test;

public class DuckDBCommandTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SetCommandText()
    {
        var cmd = new DuckDBCommand("Select 1");
        cmd.CommandText.Should().Be("Select 1");
    }

    [Fact]
    public void SetCommandTextAndConnection()
    {
        var cmd = new DuckDBCommand("Select 1", Connection);

        cmd.CommandText.Should().Be("Select 1");
        cmd.Connection.Should().Be(Connection);
    }

    // https://learn.microsoft.com/dotnet/api/system.data.idbcommand.prepare, Exceptions: InvalidOperationException,
    // "The Connection is not set. -or- The Connection is not Open()."
    [Fact]
    public void PrepareThrowsWithoutAnOpenConnection()
    {
        new DuckDBCommand("SELECT 1").Invoking(c => c.Prepare()).Should().Throw<InvalidOperationException>();

        using var closed = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        new DuckDBCommand("SELECT 1", closed).Invoking(c => c.Prepare()).Should().Throw<InvalidOperationException>();

        new DuckDBCommand("SELECT 1", Connection).Invoking(c => c.Prepare()).Should().NotThrow();
    }
}

