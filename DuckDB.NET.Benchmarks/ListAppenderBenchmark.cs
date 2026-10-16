using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// One list column, ten items in every list. The items of a list are written one at a time by the writer of the
// item type, so the time is the cost of a list plus ten times the cost of an item.
// AppendIntLists has no nullable item and is the reference for the others, where one item in ten is null.
[MemoryDiagnoser]
public class ListAppenderBenchmark
{
    private const int ListCount = 1024;
    private const int ItemCount = 10;

    // Created once, so that the benchmarks do not measure the making of the lists.
    private readonly List<int>[] intLists = new List<int>[ListCount];
    private readonly List<int?>[] nullableIntLists = new List<int?>[ListCount];
    private readonly int?[][] nullableIntArrays = new int?[ListCount][];

    // Lists of three strings, where the work done once per list weighs more.
    private readonly List<string>[] shortStringLists = new List<string>[ListCount];

    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        for (var i = 0; i < ListCount; i++)
        {
            intLists[i] = new List<int>(ItemCount);
            nullableIntLists[i] = new List<int?>(ItemCount);

            for (var item = 0; item < ItemCount; item++)
            {
                var value = i * ItemCount + item;
                var isNull = (i + item) % ItemCount == 0;

                intLists[i].Add(value);
                nullableIntLists[i].Add(isNull ? null : value);
            }

            nullableIntArrays[i] = nullableIntLists[i].ToArray();

            shortStringLists[i] = ["a" + i, "b" + i, "c" + i];
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    private void CreateTable(string itemType)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS bench; CREATE TABLE bench (l {itemType}[]);";
        command.ExecuteNonQuery();
    }

    [IterationSetup(Target = nameof(AppendShortStringLists))]
    public void StringSetup() => CreateTable("VARCHAR");

    [IterationSetup(Targets = [nameof(AppendIntLists), nameof(AppendNullableIntLists), nameof(AppendNullableIntArrays)])]
    public void IntegerSetup() => CreateTable("INTEGER");

    [Benchmark]
    public void AppendIntLists()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(intLists[i % ListCount], static (row, value) => row.AppendValue(value));
        }
    }

    [Benchmark]
    public void AppendNullableIntLists()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(nullableIntLists[i % ListCount], static (row, value) => row.AppendValue(value));
        }
    }

    [Benchmark]
    public void AppendNullableIntArrays()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(nullableIntArrays[i % ListCount], static (row, value) => row.AppendValue(value));
        }
    }

    [Benchmark]
    public void AppendShortStringLists()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow(shortStringLists[i % ListCount], static (row, value) => row.AppendValue(value));
        }
    }

}
