using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using DuckDB.NET.Data.Mapping;

namespace DuckDB.NET.Benchmarks;

// The mapped appender: the same eight INTEGER columns written from int and from int? properties, and a record of
// mixed types. Up to 1.5.6, a nullable property was boxed for every value.
[MemoryDiagnoser]
public class MappedAppenderBenchmark
{
    private DuckDBConnection connection = null!;
    private Row[] rows = null!;
    private NullableRow[] nullableRows = null!;
    private MixedRow[] mixedRows = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    public sealed class Row
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

    public sealed class NullableRow
    {
        public int? A { get; init; }
        public int? B { get; init; }
        public int? C { get; init; }
        public int? D { get; init; }
        public int? E { get; init; }
        public int? F { get; init; }
        public int? G { get; init; }
        public int? H { get; init; }
    }

    // Five columns of different kinds, as a record of an application has them.
    public sealed class MixedRow
    {
        public int Id { get; init; }
        public string Name { get; init; } = "";
        public double Score { get; init; }
        public DateTime Created { get; init; }
        public long? Parent { get; init; }
    }

    public sealed class MixedRowMap : DuckDBAppenderMap<MixedRow>
    {
        public MixedRowMap()
        {
            Map(row => row.Id);
            Map(row => row.Name);
            Map(row => row.Score);
            Map(row => row.Created);
            Map(row => row.Parent);
        }
    }

    public sealed class RowMap : DuckDBAppenderMap<Row>
    {
        public RowMap()
        {
            Map(row => row.A);
            Map(row => row.B);
            Map(row => row.C);
            Map(row => row.D);
            Map(row => row.E);
            Map(row => row.F);
            Map(row => row.G);
            Map(row => row.H);
        }
    }

    public sealed class NullableRowMap : DuckDBAppenderMap<NullableRow>
    {
        public NullableRowMap()
        {
            Map(row => row.A);
            Map(row => row.B);
            Map(row => row.C);
            Map(row => row.D);
            Map(row => row.E);
            Map(row => row.F);
            Map(row => row.G);
            Map(row => row.H);
        }
    }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        rows = new Row[RowCount];
        nullableRows = new NullableRow[RowCount];
        mixedRows = new MixedRow[RowCount];

        var start = new DateTime(2024, 1, 1);

        for (var i = 0; i < RowCount; i++)
        {
            rows[i] = new Row { A = i, B = i, C = i, D = i, E = i, F = i, G = i, H = i };

            // Every tenth value of the first property is NULL, so the NULL path runs too.
            nullableRows[i] = new NullableRow { A = i % 10 == 0 ? null : i, B = i, C = i, D = i, E = i, F = i, G = i, H = i };

            mixedRows[i] = new MixedRow { Id = i, Name = "name" + i % 1000, Score = i / 7d, Created = start.AddSeconds(i), Parent = i % 10 == 0 ? null : i - 1 };
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [IterationSetup(Target = nameof(AppendMixedProperties))]
    public void MixedSetup()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS bench; CREATE TABLE bench (id INTEGER, name VARCHAR, score DOUBLE, created TIMESTAMP, parent BIGINT);";
        command.ExecuteNonQuery();
    }

    [IterationSetup(Targets = [nameof(AppendIntProperties), nameof(AppendNullableIntProperties)])]
    public void IterationSetup()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS bench; CREATE TABLE bench (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER, f INTEGER, g INTEGER, h INTEGER);";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void AppendIntProperties()
    {
        using var appender = connection.CreateAppender<Row, RowMap>("bench");
        appender.AppendRecords(rows);
    }

    [Benchmark]
    public void AppendNullableIntProperties()
    {
        using var appender = connection.CreateAppender<NullableRow, NullableRowMap>("bench");
        appender.AppendRecords(nullableRows);
    }

    [Benchmark]
    public void AppendMixedProperties()
    {
        using var appender = connection.CreateAppender<MixedRow, MixedRowMap>("bench");
        appender.AppendRecords(mixedRows);
    }
}
