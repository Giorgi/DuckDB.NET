using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Reads of LIST, STRUCT and MAP columns through GetFieldValue<T>.
[MemoryDiagnoser]
[WarmupCount(6)]
[IterationCount(15)]
public class CompositeReaderBenchmark
{
    private DuckDBConnection connection = null!;

    [Params(200_000)]
    public int RowCount { get; set; }

    public class IntTextDouble
    {
        public int A { get; set; }
        public string? B { get; set; }
        public double C { get; set; }
    }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE bench AS SELECT " +
                              "[i, i + 1, i + 2, i + 3, i + 4]::INTEGER[5] arr, " +
                              "[i, NULL, i + 2, NULL, i + 4]::INTEGER[] with_nulls, " +
                              "range(i % 10)::INTEGER[] var_len, " +
                              "{'a': i::INTEGER, 'b': i::VARCHAR, 'c': i::DOUBLE} st, " +
                              $"MAP {{i::INTEGER: i::VARCHAR}} m FROM range({RowCount}) t(i)";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup() => connection.Dispose();

    private T ReadAll<T>(string column, Func<DuckDBDataReader, T> read)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {column} FROM bench";
        using var reader = command.ExecuteReader();

        T last = default!;
        while (reader.Read())
        {
            last = read(reader);
        }

        return last;
    }

    [Benchmark]
    public List<int> ReadArrayAsListOfInt() => ReadAll("arr", r => r.GetFieldValue<List<int>>(0));

    [Benchmark]
    public List<int?> ReadListWithNullsAsListOfNullableInt() => ReadAll("with_nulls", r => r.GetFieldValue<List<int?>>(0));

    // Lists of 0 to 9 items, so the size a list is created with changes from row to row.
    [Benchmark]
    public List<int> ReadVaryingListAsListOfInt() => ReadAll("var_len", r => r.GetFieldValue<List<int>>(0));

    [Benchmark]
    public List<long> ReadArrayAsListOfLong() => ReadAll("arr", r => r.GetFieldValue<List<long>>(0));

    [Benchmark]
    public IntTextDouble ReadStructAsClass() => ReadAll("st", r => r.GetFieldValue<IntTextDouble>(0));

    [Benchmark]
    public Dictionary<string, object> ReadStructAsDictionary() => ReadAll("st", r => r.GetFieldValue<Dictionary<string, object>>(0));

    [Benchmark]
    public Dictionary<int, string> ReadMapAsDictionary() => ReadAll("m", r => r.GetFieldValue<Dictionary<int, string>>(0));
}
