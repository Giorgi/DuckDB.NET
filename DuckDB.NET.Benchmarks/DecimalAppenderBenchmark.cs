using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Four DECIMAL columns per benchmark, written from decimal. What a write costs depends on how the scale of the
// value compares with the scale of the column, and on whether the column is stored in 64 bits or in 128.
[MemoryDiagnoser]
public class DecimalAppenderBenchmark
{
    private const int ValueCount = 1024;

    // Created once, so that the benchmarks do not measure decimal arithmetic of their own.
    private readonly decimal?[] fourDigits = new decimal?[ValueCount];      // 1234.5678: the scale of the column
    private readonly decimal?[] twoDigits = new decimal?[ValueCount];       // 1234.56: fewer digits than the column
    private readonly decimal?[] quotients = new decimal?[ValueCount];       // 1 / 7 and the like: 28 digits

    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        for (var i = 0; i < ValueCount; i++)
        {
            var sign = i % 5 == 0 ? -1 : 1;

            fourDigits[i] = sign * new decimal(12_345_678 + i * 7919, 0, 0, false, 4);
            twoDigits[i] = sign * new decimal(123_456 + i * 79, 0, 0, false, 2);
            quotients[i] = sign * (1000m + i) / 7m;
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

    [IterationSetup(Targets = [nameof(AppendSameScale), nameof(AppendSmallerScale), nameof(AppendQuotients)])]
    public void BigIntStorageSetup() => CreateTable("DECIMAL(18,4)");

    [IterationSetup(Target = nameof(AppendToWide))]
    public void WideSetup() => CreateTable("DECIMAL(38,10)");

    [Benchmark]
    public void AppendSameScale() => Append(fourDigits);

    // Scaled up to the column in 64-bit storage; AppendToWide scales up in 128 bits.
    [Benchmark]
    public void AppendSmallerScale() => Append(twoDigits);

    [Benchmark]
    public void AppendQuotients() => Append(quotients);

    [Benchmark]
    public void AppendToWide() => Append(fourDigits);

    private void Append(decimal?[] values)
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
