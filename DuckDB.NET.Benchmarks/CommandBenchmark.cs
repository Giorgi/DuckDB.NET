using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// One small command per call, the way an application or Dapper uses the provider: a constant query, a lookup of one
// row by an INTEGER or a VARCHAR parameter, positional and named, and a page of rows read with the typed getters.
// Every command is parsed and planned each time it runs, so most of the time is DuckDB's. ReusedCommand keeps one
// command and changes the value of its parameter; Prepare() does nothing in 1.5.6.
[MemoryDiagnoser]
public class CommandBenchmark
{
    private const int TableRows = 10_000;

    private DuckDBConnection connection = null!;
    private DuckDBCommand reusedCommand = null!;
    private int next;

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR, price DECIMAL(18,2), created TIMESTAMP); " +
                              "INSERT INTO items SELECT i, 'item ' || i, i * 1.25, TIMESTAMP '2024-01-01' + to_seconds(i) " +
                              $"FROM range({TableRows}) t(i);";
        command.ExecuteNonQuery();

        reusedCommand = connection.CreateCommand();
        reusedCommand.CommandText = "SELECT name FROM items WHERE id = $1";
        reusedCommand.Parameters.Add(new DuckDBParameter(0));
        reusedCommand.Prepare();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        reusedCommand.Dispose();
        connection.Dispose();
    }

    // A different row on every call.
    private int NextId() => next = (next + 7919) % TableRows;

    [Benchmark]
    public object? SelectConstant()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 42";
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object? SelectByIntParameter()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM items WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter(NextId()));
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object? SelectByNamedParameter()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM items WHERE id = $id";
        command.Parameters.Add(new DuckDBParameter("id", NextId()));
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object? SelectByStringParameter()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id FROM items WHERE name = $1";
        command.Parameters.Add(new DuckDBParameter("item " + NextId()));
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object? ReusedCommand()
    {
        reusedCommand.Parameters[0].Value = NextId();
        return reusedCommand.ExecuteScalar();
    }

    // A page of 100 rows, every column read.
    [Benchmark]
    public long ReadPage()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id, name, price, created FROM items WHERE id >= $1 ORDER BY id LIMIT 100";
        command.Parameters.Add(new DuckDBParameter(NextId() % (TableRows - 100)));
        using var reader = command.ExecuteReader();

        var hash = 0L;
        while (reader.Read())
        {
            hash += reader.GetInt32(0) + reader.GetString(1).Length + (long)reader.GetDecimal(2) + reader.GetDateTime(3).Ticks;
        }

        return hash;
    }
}
