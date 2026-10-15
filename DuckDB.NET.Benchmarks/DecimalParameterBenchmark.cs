using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Decimals bound as parameters. A command is parsed and planned every time it runs, which costs far more than
// converting its parameters, so the first two cases show what a user sees. The list cases bind one parameter of
// ListSize decimals, where the conversion of the items is most of the work on the .NET side.
// InsertLongs binds four BIGINT parameters and is the reference.
[MemoryDiagnoser]
public class DecimalParameterBenchmark
{
    private const int ValueCount = 1024;
    private const int ListSize = 10_000;

    private readonly decimal[] values = new decimal[ValueCount];
    private readonly List<decimal> list = new(ListSize);

    private DuckDBConnection connection = null!;

    [Params(1_000)]
    public int Executions { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        for (var i = 0; i < ValueCount; i++)
        {
            values[i] = (i % 5 == 0 ? -1 : 1) * new decimal(12_345_678 + i * 7919, 0, 0, false, 4);
        }

        for (var i = 0; i < ListSize; i++)
        {
            list.Add(values[i % ValueCount]);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [IterationSetup]
    public void CreateTables()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS bench; DROP TABLE IF EXISTS bench_longs; DROP TABLE IF EXISTS bench_lists; " +
                              "CREATE TABLE bench (a DECIMAL(18,4), b DECIMAL(18,4), c DECIMAL(18,4), d DECIMAL(18,4)); " +
                              "CREATE TABLE bench_longs (a BIGINT, b BIGINT, c BIGINT, d BIGINT); " +
                              "CREATE TABLE bench_lists (l DECIMAL(18,4)[]);";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void InsertDecimals()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO bench VALUES ($1, $2, $3, $4)";

        for (var i = 0; i < 4; i++)
        {
            command.Parameters.Add(new DuckDBParameter());
        }

        for (var i = 0; i < Executions; i++)
        {
            var value = values[i % ValueCount];

            for (var p = 0; p < 4; p++)
            {
                command.Parameters[p].Value = value;
            }

            command.ExecuteNonQuery();
        }
    }

    [Benchmark]
    public void InsertLongs()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO bench_longs VALUES ($1, $2, $3, $4)";

        for (var i = 0; i < 4; i++)
        {
            command.Parameters.Add(new DuckDBParameter());
        }

        for (var i = 0; i < Executions; i++)
        {
            long value = i;

            for (var p = 0; p < 4; p++)
            {
                command.Parameters[p].Value = value;
            }

            command.ExecuteNonQuery();
        }
    }

    // One parameter of 10,000 decimals for a list column, 20 times.
    [Benchmark]
    public void InsertDecimalLists()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO bench_lists VALUES ($1)";
        command.Parameters.Add(new DuckDBParameter(list));

        for (var i = 0; i < 20; i++)
        {
            command.ExecuteNonQuery();
        }
    }

    // The same list with no column to give it a type, 20 times.
    [Benchmark]
    public object? SelectFromDecimalLists()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT len($1)";
        command.Parameters.Add(new DuckDBParameter(list));

        object? result = null;

        for (var i = 0; i < 20; i++)
        {
            result = command.ExecuteScalar();
        }

        return result;
    }
}
