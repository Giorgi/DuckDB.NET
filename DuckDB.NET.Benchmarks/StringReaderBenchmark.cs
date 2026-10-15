using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// One VARCHAR column per benchmark, read with GetString. The columns differ in length and in whether the text is
// ASCII, because decoding costs depend on both. DuckDB keeps a string of up to 12 bytes inside the vector.
[MemoryDiagnoser]
public class StringReaderBenchmark
{
    private DuckDBConnection connection = null!;

    [Params(200_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE bench AS SELECT " +
                              "i::VARCHAR AS short_ascii, " +                                       // 1 to 6 bytes, inline
                              "'customer-' || md5(i::VARCHAR) AS medium_ascii, " +                 // 41 bytes
                              "repeat(md5(i::VARCHAR), 16) AS long_ascii, " +                      // 512 bytes
                              "'ქართული 🦆 ' || i::VARCHAR AS short_unicode, " +                   // about 30 bytes, 3 and 4 byte characters
                              "repeat('ქართული ტექსტი 🦆 ', 12) || i::VARCHAR AS long_unicode, " + // about 560 bytes
                              "CASE i % 5 WHEN 0 THEN '' WHEN 1 THEN i::VARCHAR WHEN 2 THEN md5(i::VARCHAR) " +
                              "WHEN 3 THEN repeat(md5(i::VARCHAR), 16) ELSE 'ქართული ტექსტი 🦆 ' || i::VARCHAR END AS mixed " +
                              $"FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    private long Read(string column)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {column} FROM bench";
        using var reader = command.ExecuteReader();

        var length = 0L;
        while (reader.Read())
        {
            length += reader.GetString(0).Length;
        }

        return length;
    }

    [Benchmark]
    public long ShortAscii() => Read("short_ascii");

    [Benchmark]
    public long MediumAscii() => Read("medium_ascii");

    [Benchmark]
    public long LongAscii() => Read("long_ascii");

    [Benchmark]
    public long ShortUnicode() => Read("short_unicode");

    [Benchmark]
    public long LongUnicode() => Read("long_unicode");

    [Benchmark]
    public long Mixed() => Read("mixed");
}
