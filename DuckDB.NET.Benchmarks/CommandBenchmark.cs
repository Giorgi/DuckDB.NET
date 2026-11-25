using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// One small command per call, the way an application or Dapper uses the provider: a constant query, a lookup of one
// row by an INTEGER or a VARCHAR parameter, positional and named, an expression, a one-row insert, and a page of rows
// read with the typed getters. Every new command is parsed and prepared each time it runs, so most of the time is
// DuckDB's. The Reused* benchmarks keep one prepared command and change the value of its parameters: since 2.0,
// Prepare() keeps the statement, so they skip the parse and the prepare. In the released package before 2.0,
// Prepare() does nothing, so the same benchmarks measure an unprepared command there; that package also runs an older
// DuckDB engine, so what Prepare() saves is read inside one job: SelectExpression against ReusedExpression, InsertRow
// against ReusedInsert, SelectByIntParameter against ReusedCommand. On DuckDB 2.0 a statement that reads a table is
// still planned on every execution (duckdb#24696); the expression and the insert are not.
[MemoryDiagnoser]
public class CommandBenchmark
{
    private const int TableRows = 10_000;

    private DuckDBConnection connection = null!;
    private DuckDBCommand reusedCommand = null!;
    private DuckDBCommand reusedExpression = null!;
    private DuckDBCommand reusedInsert = null!;
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

        command.CommandText = "CREATE TABLE log (id INTEGER, message VARCHAR)";
        command.ExecuteNonQuery();

        reusedCommand = connection.CreateCommand();
        reusedCommand.CommandText = "SELECT name FROM items WHERE id = $1";
        reusedCommand.Parameters.Add(new DuckDBParameter(0));
        reusedCommand.Prepare();

        reusedExpression = connection.CreateCommand();
        reusedExpression.CommandText = "SELECT $1::INTEGER + 1";
        reusedExpression.Parameters.Add(new DuckDBParameter(0));
        reusedExpression.Prepare();

        reusedInsert = connection.CreateCommand();
        reusedInsert.CommandText = "INSERT INTO log VALUES ($1, $2)";
        reusedInsert.Parameters.Add(new DuckDBParameter(0));
        reusedInsert.Parameters.Add(new DuckDBParameter("message"));
        reusedInsert.Prepare();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        reusedCommand.Dispose();
        reusedExpression.Dispose();
        reusedInsert.Dispose();
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

    [Benchmark]
    public object? SelectExpression()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER + 1";
        command.Parameters.Add(new DuckDBParameter(NextId()));
        return command.ExecuteScalar();
    }

    [Benchmark]
    public object? ReusedExpression()
    {
        reusedExpression.Parameters[0].Value = NextId();
        return reusedExpression.ExecuteScalar();
    }

    [Benchmark]
    public int InsertRow()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO log VALUES ($1, $2)";
        command.Parameters.Add(new DuckDBParameter(NextId()));
        command.Parameters.Add(new DuckDBParameter("message"));
        return command.ExecuteNonQuery();
    }

    [Benchmark]
    public int ReusedInsert()
    {
        reusedInsert.Parameters[0].Value = NextId();
        return reusedInsert.ExecuteNonQuery();
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
