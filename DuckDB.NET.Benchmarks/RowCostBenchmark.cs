using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// The same INTEGER value written to tables of one and eight columns. The time of a row is a fixed part plus a part
// for each column, so the two results give both: the fixed part is what a row costs before it has written anything. AppendRow and CreateRow are the two ways to make a row.
[MemoryDiagnoser]
public class RowCostBenchmark
{
    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [Params(1, 8)]
    public int Columns { get; set; }

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
        command.CommandText = $"DROP TABLE IF EXISTS bench; CREATE TABLE bench ({string.Join(", ", Enumerable.Range(0, Columns).Select(i => $"c{i} INTEGER"))});";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void AppendRow()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((Value: (int?)i, Columns), static (row, state) =>
            {
                for (var column = 0; column < state.Columns; column++)
                {
                    row.AppendValue(state.Value);
                }
            });
        }
    }

    [Benchmark]
    public void CreateRow()
    {
        using var appender = connection.CreateAppender("bench");
        var columns = Columns;

        for (var i = 0; i < RowCount; i++)
        {
            int? value = i;
            var row = appender.CreateRow();

            for (var column = 0; column < columns; column++)
            {
                row.AppendValue(value);
            }

            row.EndRow();
        }
    }
}
