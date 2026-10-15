using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Eight numeric columns of eight types and nothing else, so the time is dominated by the per-value write path.
// RowCostBenchmark writes eight INTEGER columns.
[MemoryDiagnoser]
public class NumericAppenderBenchmark
{
    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [IterationSetup]
    public void CreateTable()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS bench; CREATE TABLE bench (a TINYINT, b SMALLINT, c INTEGER, d BIGINT, e UINTEGER, f UBIGINT, g FLOAT, h DOUBLE);";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void AppendMixedNumerics()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(i, static (row, value) =>
            {
                row.AppendValue((sbyte)(value % 100))
                    .AppendValue((short)(value % 1000))
                    .AppendValue(value)
                    .AppendValue((long)value)
                    .AppendValue((uint)value)
                    .AppendValue((ulong)value)
                    .AppendValue((float)value)
                    .AppendValue((double)value);
            });
        }
    }
}
