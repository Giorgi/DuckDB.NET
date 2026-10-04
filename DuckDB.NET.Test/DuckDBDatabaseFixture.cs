namespace DuckDB.NET.Test;

public class DuckDBDatabaseFixture : IDisposable
{
    public DuckDBDatabaseFixture()
    {
        Connection = new DuckDBConnection("DataSource=:memory:");
        Connection.Open();
    }

    public void Dispose()
    {
        Connection.Dispose();
    }

    internal DuckDBConnection Connection { get; }
}

public class DuckDBTestBase : IDisposable, IClassFixture<DuckDBDatabaseFixture>
{
    protected DuckDBCommand Command { get; }
    protected DuckDBConnection Connection { get; }

    protected Faker Faker { get; init; } = new Faker();

    protected List<T> GetRandomList<T>(Func<Faker, T> generator, int? count = 20)
    {
        return Enumerable.Range(0, count ?? Faker.Random.Int(0, 50)).Select(i => generator(Faker)).ToList();
    }

    public DuckDBTestBase(DuckDBDatabaseFixture db)
    {
        Connection = db.Connection;

        if (Connection.State == ConnectionState.Closed)
        {
            Connection.Open();
        }

        Command = db.Connection.CreateCommand();
    }

    public virtual void Dispose()
    {
        Command?.Dispose();
    }

    /// <summary>
    /// Opens a private in-memory database for tests where a streaming query must fail only after some rows were read.
    /// One thread produces the chunks in order, and a small streaming buffer lets it get only a few chunks ahead of
    /// the reader, so the rows before the failing one are read first. With the default settings DuckDB 2.0 buffers
    /// past the failing row and reports the error on the first fetch, and DuckDB 1.5 can report such an error as an
    /// interrupt when several threads are running.
    /// </summary>
    protected static DuckDBConnection OpenConnectionForLateStreamingErrors()
    {
        var connection = new DuckDBConnection("DataSource=:memory:;threads=1");
        connection.Open();

        // A per-connection setting, so it cannot go into the connection string.
        using var command = connection.CreateCommand();
        command.CommandText = "SET streaming_buffer_size = '100KB'";
        command.ExecuteNonQuery();

        return connection;
    }
}