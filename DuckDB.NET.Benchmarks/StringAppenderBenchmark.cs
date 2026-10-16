using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Four VARCHAR columns per benchmark, written from string. What a write costs depends on the length in UTF-8:
// DuckDB keeps a string of up to 12 bytes inside its 16-byte value, and a longer one in a heap of the vector.
// 500,000 rows, not a million: four columns of 300 characters would make a table of more than a gigabyte.
[MemoryDiagnoser]
public class StringAppenderBenchmark
{
    private const int ValueCount = 1024;

    // Created once, so that the benchmarks do not measure the building of strings.
    private readonly string[] shortAscii = new string[ValueCount];      // 5 to 8 characters
    private readonly string[] mediumAscii = new string[ValueCount];     // 40 characters
    private readonly string[] longAscii = new string[ValueCount];       // 300 characters
    private readonly string[] shortGeorgian = new string[ValueCount];   // 4 characters, 12 bytes: inline
    private readonly string[] mediumGeorgian = new string[ValueCount];  // 30 characters, 90 bytes

    private DuckDBConnection connection = null!;

    [Params(500_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        for (var i = 0; i < ValueCount; i++)
        {
            // The Georgian letters start at U+10D0 and take three bytes each.
            var letter = (char)('ა' + i % 33);

            shortAscii[i] = "item" + i;
            mediumAscii[i] = "Customer number " + i.ToString("D4") + " of the eastern region";
            longAscii[i] = new string((char)('a' + i % 26), 296) + i.ToString("D4");
            shortGeorgian[i] = new string(letter, 4);
            mediumGeorgian[i] = new string(letter, 30);
        }
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
        command.CommandText = "DROP TABLE IF EXISTS bench; CREATE TABLE bench (a VARCHAR, b VARCHAR, c VARCHAR, d VARCHAR);";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void AppendShortAscii() => Append(shortAscii);

    [Benchmark]
    public void AppendMediumAscii() => Append(mediumAscii);

    [Benchmark]
    public void AppendLongAscii() => Append(longAscii);

    // Inline like AppendShortAscii, but every character takes three bytes: 12 bytes, the most that is kept inline.
    [Benchmark]
    public void AppendShortGeorgian() => Append(shortGeorgian);

    [Benchmark]
    public void AppendMediumGeorgian() => Append(mediumGeorgian);

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
