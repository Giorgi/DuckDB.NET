using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using DuckDB.NET.Data.Mapping;

namespace DuckDB.NET.Benchmarks;

[MemoryDiagnoser]
public class MappedAppenderBenchmark
{
    private DuckDBConnection connection = null!;
    private BenchRow[] rows = null!;
    private IPropertyMapping<BenchRow>[] mappings = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    public sealed class BenchRow
    {
        public int Id { get; init; }
        public long Score { get; init; }
        public double Value { get; init; }
        public bool Active { get; init; }
    }

    public sealed class BenchRowMap : DuckDBAppenderMap<BenchRow>
    {
        public BenchRowMap()
        {
            Map(row => row.Id);
            Map(row => row.Score);
            Map(row => row.Value);
            Map(row => row.Active);
        }
    }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        rows = new BenchRow[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            rows[i] = new BenchRow { Id = i, Score = i, Value = i, Active = i % 2 == 0 };
        }

        mappings = new BenchRowMap().PropertyMappings.ToArray();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS bench_mapped; CREATE TABLE bench_mapped (a INTEGER, b BIGINT, c DOUBLE, d BOOLEAN);";
        command.ExecuteNonQuery();
    }

    // Mirrors the mapped appender loop before it adopted AppendRow.
    [Benchmark(Baseline = true)]
    public void AppendMappedRowsWithCreateRow()
    {
        using var appender = connection.CreateAppender("bench_mapped");

        foreach (var record in rows)
        {
            AppendRecordWithCreateRow(appender, record);
        }
    }

    [Benchmark]
    public void AppendMappedRowsWithAppendRow()
    {
        using var appender = connection.CreateAppender<BenchRow, BenchRowMap>("bench_mapped");
        appender.AppendRecords(rows);
    }

    private void AppendRecordWithCreateRow(DuckDBAppender appender, BenchRow record)
    {
        ArgumentNullException.ThrowIfNull(record);

        var row = appender.CreateRow();
        foreach (var mapping in mappings)
        {
            mapping.AppendToRow(row, record);
        }

        row.EndRow();
    }
}
