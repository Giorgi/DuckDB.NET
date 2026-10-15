using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Scalar and table functions written in C#. Each scalar function is summed over a table of RowCount rows, so the
// time is the cost per value of reading the arguments, calling the function and writing the result.
// SumInSql is the same sum with no function, as the reference. One thread, so that the times compare.
[MemoryDiagnoser]
public class FunctionBenchmark
{
    private DuckDBConnection connection = null!;
    private Row[] rows = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    // A class: up to 1.5.6, a struct row was boxed on its way to the mapper of the table function.
    public sealed class Row
    {
        public int Id { get; init; }
        public long Value { get; init; }
        public double Ratio { get; init; }
        public int? Optional { get; init; }
    }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        Execute("SET threads = 1");
        Execute($"CREATE TABLE numbers AS SELECT i::INTEGER AS i, (i * 3)::INTEGER AS j, CASE WHEN i % 10 = 0 THEN NULL ELSE i::INTEGER END AS n, " +
                $"i::DOUBLE / 7 AS d, 'value ' || (i % 1000) AS s FROM range({RowCount}) t(i)");

        // The function is given the readers and the writer, and loops over the rows itself.
        connection.RegisterScalarFunction<int, int>("low_level_plus_one", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                writer.WriteValue(readers[0].GetValue<int>(index) + 1, index);
            }
        });

        // The functions below are given one row at a time, and the library reads and writes.
        connection.RegisterScalarFunction<int, int>("plus_one", value => value + 1);
        connection.RegisterScalarFunction<int?, int?>("nullable_plus_one", value => value + 1);
        connection.RegisterScalarFunction<int, int?>("int_to_nullable", value => value + 1);
        connection.RegisterScalarFunction<int, int, int>("add", (left, right) => left + right);
        connection.RegisterScalarFunction<double, double>("half", value => value / 2);
        connection.RegisterScalarFunction<string, int>("text_length", (string value) => value.Length);

        rows = new Row[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            rows[i] = new Row { Id = i, Value = i * 3L, Ratio = i / 7d, Optional = i % 10 == 0 ? null : i };
        }

        connection.RegisterTableFunction("bench_rows", () => rows, row => new { row.Id, row.Value, row.Ratio, row.Optional });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public object? SumInSql() => Scalar("SELECT sum(i + 1) FROM numbers");

    [Benchmark]
    public object? LowLevelIntFunction() => Scalar("SELECT sum(low_level_plus_one(i)) FROM numbers");

    [Benchmark]
    public object? IntFunction() => Scalar("SELECT sum(plus_one(i)) FROM numbers");

    [Benchmark]
    public object? NullableIntFunction() => Scalar("SELECT sum(nullable_plus_one(n)) FROM numbers");

    // A nullable result from an argument that is not nullable: what the result costs, apart from the argument.
    [Benchmark]
    public object? IntToNullableFunction() => Scalar("SELECT sum(int_to_nullable(i)) FROM numbers");

    [Benchmark]
    public object? TwoIntFunction() => Scalar("SELECT sum(add(i, j)) FROM numbers");

    [Benchmark]
    public object? DoubleFunction() => Scalar("SELECT sum(half(d)) FROM numbers");

    [Benchmark]
    public object? StringFunction() => Scalar("SELECT sum(text_length(s)) FROM numbers");

    // Four columns of a table function, all of them read.
    [Benchmark]
    public object? TableFunction() => Scalar("SELECT sum(Id), sum(Value), sum(Ratio), sum(Optional) FROM bench_rows()");

    // One column of the same function: only the columns a query reads are written.
    [Benchmark]
    public object? TableFunctionOneColumn() => Scalar("SELECT sum(Id) FROM bench_rows()");

    private object? Scalar(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private void Execute(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
