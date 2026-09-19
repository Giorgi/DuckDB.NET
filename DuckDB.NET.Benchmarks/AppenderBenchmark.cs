using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

[MemoryDiagnoser]
public class AppenderBenchmark
{
    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
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
        command.CommandText = "DROP TABLE IF EXISTS bench; CREATE TABLE bench (a INTEGER, b BIGINT, c DOUBLE, d BOOLEAN);";
        command.ExecuteNonQuery();
    }

    [Benchmark(Baseline = true)]
    public void AppendRowsWithCreateRow()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.CreateRow()
                .AppendValue(i)
                .AppendValue((long)i)
                .AppendValue((double)i)
                .AppendValue(i % 2 == 0)
                .EndRow();
        }
    }

    [Benchmark]
    public void AppendRowsWithAppendRow()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(i, static (row, value) =>
            {
                row.AppendValue(value)
                    .AppendValue((long)value)
                    .AppendValue((double)value)
                    .AppendValue(value % 2 == 0);
            });
        }
    }
}
