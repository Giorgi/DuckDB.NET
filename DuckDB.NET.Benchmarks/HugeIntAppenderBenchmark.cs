using System.Numerics;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Four HUGEINT or UHUGEINT columns per benchmark, written from BigInteger. Each value goes through the
// DuckDBHugeInt or DuckDBUHugeInt constructor, which is what these benchmarks are for.
[MemoryDiagnoser]
public class HugeIntAppenderBenchmark
{
    private const int ValueCount = 1024;

    // Created once, so that the benchmarks do not measure BigInteger arithmetic.
    private readonly BigInteger?[] small = new BigInteger?[ValueCount];
    private readonly BigInteger?[] large = new BigInteger?[ValueCount];
    private readonly BigInteger?[] largeUnsigned = new BigInteger?[ValueCount];

    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        var random = new Random(42);
        var bytes = new byte[16];

        for (var i = 0; i < ValueCount; i++)
        {
            // A BigInteger that fits an int keeps no array of digits, so it takes another path in every conversion.
            small[i] = random.Next(int.MinValue, int.MaxValue);

            random.NextBytes(bytes);
            bytes[15] |= 0x40; // above 2^126 in size, so both halves are in use

            large[i] = new BigInteger(bytes); // signed: about half of them are negative
            largeUnsigned[i] = new BigInteger(bytes, isUnsigned: true);
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

    [IterationSetup(Targets = [nameof(AppendSmallToHugeInt), nameof(AppendLargeToHugeInt)])]
    public void HugeIntSetup() => CreateTable("HUGEINT");

    [IterationSetup(Target = nameof(AppendLargeToUHugeInt))]
    public void UHugeIntSetup() => CreateTable("UHUGEINT");

    [Benchmark]
    public void AppendSmallToHugeInt() => Append(small);

    [Benchmark]
    public void AppendLargeToHugeInt() => Append(large);

    [Benchmark]
    public void AppendLargeToUHugeInt() => Append(largeUnsigned);

    private void Append(BigInteger?[] values)
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
