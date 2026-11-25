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

    public record struct IntTextDoubleStruct(int A, string? B, double C);

    public class FourTexts
    {
        public string? A { get; set; }
        public string? B { get; set; }
        public string? C { get; set; }
        public string? D { get; set; }
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
                              "MAP {i::INTEGER: i::VARCHAR} m, " +
                              "['a' || i, 'b' || i, 'c' || i, 'd' || i, 'e' || i] strs, " +
                              "[TIMESTAMP '2024-01-01' + to_seconds(i), TIMESTAMP '2024-01-02' + to_seconds(i), TIMESTAMP '2024-01-03' + to_seconds(i)] stamps, " +
                              "[i, NULL, i + 2, NULL, i + 4]::BIGINT[] longs_with_nulls, " +
                              "[[i, i + 1]::INTEGER[], [i + 2]::INTEGER[]] nested, " +
                              "{'a': 'a' || i, 'b': 'b' || i, 'c': 'c' || i, 'd': 'd' || i} st4, " +
                              "map_from_entries([(i::INTEGER + k, (i + k)::VARCHAR) FOR k IN range(10)]) m10, " +
                              "MAP {(i % 1000)::SMALLINT: (i / 100)::DECIMAL(9, 2)} mdec " +
                              $"FROM range({RowCount}) t(i)";
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

    [Benchmark]
    public List<string> ReadListOfStrings() => ReadAll("strs", r => r.GetFieldValue<List<string>>(0));

    [Benchmark]
    public List<DateTime> ReadListOfTimestamps() => ReadAll("stamps", r => r.GetFieldValue<List<DateTime>>(0));

    [Benchmark]
    public List<long?> ReadListWithNullsAsListOfNullableLong() => ReadAll("longs_with_nulls", r => r.GetFieldValue<List<long?>>(0));

    [Benchmark]
    public List<List<int>> ReadNestedList() => ReadAll("nested", r => r.GetFieldValue<List<List<int>>>(0));

    // Up to 2.0, a STRUCT read into a C# struct gave every property at its default under the JIT, so before that
    // only the time of this case means anything.
    [Benchmark]
    public IntTextDoubleStruct ReadStructAsRecordStruct() => ReadAll("st", r => r.GetFieldValue<IntTextDoubleStruct>(0));

    [Benchmark]
    public FourTexts ReadStructOfStringsAsClass() => ReadAll("st4", r => r.GetFieldValue<FourTexts>(0));

    [Benchmark]
    public Dictionary<int, string> ReadMapOfTenAsDictionary() => ReadAll("m10", r => r.GetFieldValue<Dictionary<int, string>>(0));

    [Benchmark]
    public Dictionary<long, string> ReadMapWithWiderKey() => ReadAll("m", r => r.GetFieldValue<Dictionary<long, string>>(0));

    [Benchmark]
    public Dictionary<short, decimal> ReadMapOfSmallIntToDecimal() => ReadAll("mdec", r => r.GetFieldValue<Dictionary<short, decimal>>(0));
}
