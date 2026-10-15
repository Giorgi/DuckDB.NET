using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Eight columns of eight different integer types in one row, so each call in the reader sees a different
// column type on every column. DataReaderBenchmark reads eight columns of one type.
[MemoryDiagnoser]
public class MixedNumericReaderBenchmark
{
    private const string Query = "SELECT a, b, c, d, e, f, g, h FROM mixed";

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
        command.CommandText = "CREATE TABLE mixed AS SELECT (i % 100)::TINYINT a, (i % 100)::SMALLINT b, i::INTEGER c, i::BIGINT d, " +
                              $"(i % 100)::UTINYINT e, (i % 100)::USMALLINT f, i::UINTEGER g, i::UBIGINT h FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public long ReadAsColumnType()
    {
        using var command = connection.CreateCommand();
        command.CommandText = Query;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetFieldValue<sbyte>(0) + reader.GetInt16(1) + reader.GetInt32(2) + reader.GetInt64(3) +
                   reader.GetByte(4) + reader.GetFieldValue<ushort>(5) + reader.GetFieldValue<uint>(6) + (long)reader.GetFieldValue<ulong>(7);
        }

        return sum;
    }

    [Benchmark]
    public long ReadAsLong()
    {
        using var command = connection.CreateCommand();
        command.CommandText = Query;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetInt64(0) + reader.GetInt64(1) + reader.GetInt64(2) + reader.GetInt64(3) +
                   reader.GetInt64(4) + reader.GetInt64(5) + reader.GetInt64(6) + reader.GetInt64(7);
        }

        return sum;
    }

    [Benchmark]
    public double ReadAsDouble()
    {
        using var command = connection.CreateCommand();
        command.CommandText = Query;
        using var reader = command.ExecuteReader();

        var sum = 0d;
        while (reader.Read())
        {
            sum += reader.GetDouble(0) + reader.GetDouble(1) + reader.GetDouble(2) + reader.GetDouble(3) +
                   reader.GetDouble(4) + reader.GetDouble(5) + reader.GetDouble(6) + reader.GetDouble(7);
        }

        return sum;
    }

    // The object path, which Dapper uses.
    [Benchmark]
    public int ReadAsObject()
    {
        using var command = connection.CreateCommand();
        command.CommandText = Query;
        using var reader = command.ExecuteReader();

        var hash = 0;
        while (reader.Read())
        {
            for (var i = 0; i < 8; i++)
            {
                hash ^= reader.GetValue(i).GetHashCode();
            }
        }

        return hash;
    }
}
