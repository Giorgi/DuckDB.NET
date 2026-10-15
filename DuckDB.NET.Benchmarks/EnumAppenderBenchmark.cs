using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Four ENUM columns per benchmark, written by name. The writer of an ENUM column reads the names of the type from
// DuckDB the first time a name is written, so what a write costs depends on how long that table of names lives
// and on how many names the type has. AppendLongs writes four BIGINT columns: it is the reference here and the
// control of the appender for the whole run.
[MemoryDiagnoser]
public class EnumAppenderBenchmark
{
    private const int ValueCount = 1024;

    private readonly string[] fewNames = new string[ValueCount];
    private readonly string[] manyNames = new string[ValueCount];
    private readonly long?[] longs = new long?[ValueCount];
    private readonly FewNames[] csharpEnums = new FewNames[ValueCount];

    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TYPE bench_few AS ENUM ({string.Join(", ", Enumerable.Range(0, 5).Select(i => $"'name{i}'"))}); " +
                              $"CREATE TYPE bench_many AS ENUM ({string.Join(", ", Enumerable.Range(0, 200).Select(i => $"'name{i}'"))});";
        command.ExecuteNonQuery();

        for (var i = 0; i < ValueCount; i++)
        {
            fewNames[i] = "name" + i % 5;
            manyNames[i] = "name" + i % 200;
            longs[i] = 12_345_678L + i * 7919;
            csharpEnums[i] = (FewNames)(i % 5);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    private void CreateTable(string columnType)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS bench; CREATE TABLE bench (a {columnType}, b {columnType}, c {columnType}, d {columnType});";
        command.ExecuteNonQuery();
    }

    [IterationSetup(Target = nameof(AppendFiveNames))]
    public void FewSetup() => CreateTable("bench_few");

    [IterationSetup(Target = nameof(AppendTwoHundredNames))]
    public void ManySetup() => CreateTable("bench_many");

    [IterationSetup(Target = nameof(AppendCSharpEnums))]
    public void CSharpEnumSetup() => CreateTable("bench_few");

    [IterationSetup(Target = nameof(AppendLongs))]
    public void LongSetup() => CreateTable("BIGINT");

    [Benchmark]
    public void AppendFiveNames() => Append(fewNames);

    [Benchmark]
    public void AppendTwoHundredNames() => Append(manyNames);

    // The same five names, from a C# enum: the value is written as its number.
    [Benchmark]
    public void AppendCSharpEnums()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(csharpEnums[i % ValueCount], static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    [Benchmark]
    public void AppendLongs()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(longs[i % ValueCount], static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    private void Append(string[] values)
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(values[i % ValueCount], static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }
}

public enum FewNames : byte { Name0, Name1, Name2, Name3, Name4 }
