using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// A table function that returns a million rows of one and eight INTEGER columns. The time of a row is a fixed part
// plus a part for each column, so the two results give both. The rows are instances of a class in one case and
// values of a struct in the other: up to 1.5.6, a table function got its rows as object, so a struct was boxed for
// every row. EnumeratedRows returns the class rows from an iterator and not as an array, so they can only be
// enumerated. SqlTable sums the same columns of a table and is what DuckDB itself needs for the query.
[MemoryDiagnoser]
public class TableFunctionRowBenchmark
{
    private DuckDBConnection connection = null!;
    private ClassRow[] classRows = null!;
    private StructRow[] structRows = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [Params(1, 8)]
    public int Columns { get; set; }

    public sealed class ClassRow
    {
        public int A { get; init; }
        public int B { get; init; }
        public int C { get; init; }
        public int D { get; init; }
        public int E { get; init; }
        public int F { get; init; }
        public int G { get; init; }
        public int H { get; init; }
    }

    public struct StructRow
    {
        public int A { get; init; }
        public int B { get; init; }
        public int C { get; init; }
        public int D { get; init; }
        public int E { get; init; }
        public int F { get; init; }
        public int G { get; init; }
        public int H { get; init; }
    }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        Execute("SET threads = 1");
        Execute($"CREATE TABLE numbers AS SELECT {string.Join(", ", "ABCDEFGH".Select(name => $"i::INTEGER AS {name}"))} FROM range({RowCount}) t(i)");

        classRows = new ClassRow[RowCount];
        structRows = new StructRow[RowCount];

        for (var i = 0; i < RowCount; i++)
        {
            classRows[i] = new ClassRow { A = i, B = i, C = i, D = i, E = i, F = i, G = i, H = i };
            structRows[i] = new StructRow { A = i, B = i, C = i, D = i, E = i, F = i, G = i, H = i };
        }

        connection.RegisterTableFunction("class_rows_1", () => classRows, row => new { row.A });
        connection.RegisterTableFunction("class_rows_8", () => classRows, row => new { row.A, row.B, row.C, row.D, row.E, row.F, row.G, row.H });

        connection.RegisterTableFunction("enumerated_rows_1", () => Enumerate(classRows), row => new { row.A });
        connection.RegisterTableFunction("enumerated_rows_8", () => Enumerate(classRows), row => new { row.A, row.B, row.C, row.D, row.E, row.F, row.G, row.H });

        connection.RegisterTableFunction("struct_rows_1", () => structRows, row => new { row.A });
        connection.RegisterTableFunction("struct_rows_8", () => structRows, row => new { row.A, row.B, row.C, row.D, row.E, row.F, row.G, row.H });
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    private string Sums => string.Join(", ", "ABCDEFGH".Take(Columns).Select(name => $"sum({name})"));

    [Benchmark]
    public object? SqlTable() => Scalar($"SELECT {Sums} FROM numbers");

    [Benchmark]
    public object? ClassRows() => Scalar($"SELECT {Sums} FROM class_rows_{Columns}()");

    [Benchmark]
    public object? EnumeratedRows() => Scalar($"SELECT {Sums} FROM enumerated_rows_{Columns}()");

    [Benchmark]
    public object? StructRows() => Scalar($"SELECT {Sums} FROM struct_rows_{Columns}()");

    private static IEnumerable<ClassRow> Enumerate(ClassRow[] rows)
    {
        foreach (var row in rows)
        {
            yield return row;
        }
    }

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
