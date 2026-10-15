using System.Numerics;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Typed reads that DataReaderBenchmark does not have: decimals, UUIDs, a nullable read that converts,
// HUGEINT and VARINT. A HUGEINT read as long is what reading the sum of an integer column looks like; read through
// GetValue or as BigInteger, it is converted to a BigInteger. A DECIMAL of more than 18 digits is stored as a HUGEINT.
[MemoryDiagnoser]
public class TypedReaderBenchmark
{
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
        command.CommandText = "CREATE TABLE typed AS SELECT " +
                              "(i / 100)::DECIMAL(18,4) d1, (i / 7)::DECIMAL(18,4) d2, (i * 3)::DECIMAL(18,4) d3, i::DECIMAL(18,4) d4, " +
                              "uuid() u1, uuid() u2, uuid() u3, uuid() u4, " +
                              "i::INTEGER n1, (i + 1)::INTEGER n2, (i + 2)::INTEGER n3, (i + 3)::INTEGER n4, " +
                              "i::HUGEINT h1, (i + 1)::HUGEINT h2, (i + 2)::HUGEINT h3, (i + 3)::HUGEINT h4, " +
                              // Beyond 64 bits, so that both halves of the stored value are used.
                              "i::HUGEINT * 100000000000000000000::HUGEINT b1, (i + 1)::HUGEINT * 100000000000000000000::HUGEINT b2, " +
                              "-(i::HUGEINT * 100000000000000000000::HUGEINT) b3, (i + 3)::HUGEINT * 100000000000000000000::HUGEINT b4, " +
                              "(i / 100)::DECIMAL(38,4) w1, (i / 7)::DECIMAL(38,4) w2, (i * 3)::DECIMAL(38,4) w3, i::DECIMAL(38,4) w4, " +
                              // Positive and negative, most of them beyond 64 bits.
                              "(i::HUGEINT * 99999999999999999::HUGEINT - 50000000000000000::HUGEINT)::VARINT v1, " +
                              "(-(i::HUGEINT * 99999999999999999::HUGEINT))::VARINT v2 " +
                              $"FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public decimal ReadDecimals()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT d1, d2, d3, d4 FROM typed";
        using var reader = command.ExecuteReader();

        var sum = 0m;
        while (reader.Read())
        {
            sum += reader.GetDecimal(0) + reader.GetDecimal(1) + reader.GetDecimal(2) + reader.GetDecimal(3);
        }

        return sum;
    }

    [Benchmark]
    public int ReadGuids()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT u1, u2, u3, u4 FROM typed";
        using var reader = command.ExecuteReader();

        var hash = 0;
        while (reader.Read())
        {
            hash ^= reader.GetGuid(0).GetHashCode() ^ reader.GetGuid(1).GetHashCode() ^ reader.GetGuid(2).GetHashCode() ^ reader.GetGuid(3).GetHashCode();
        }

        return hash;
    }

    // INTEGER read as long?: the conversion happens inside the reader's handling of the nullable type.
    [Benchmark]
    public long ReadIntegersAsNullableLong()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT n1, n2, n3, n4 FROM typed";
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            for (var i = 0; i < 4; i++)
            {
                sum += reader.GetFieldValue<long?>(i) ?? 0;
            }
        }

        return sum;
    }

    // The object path, which Dapper uses: every value becomes a BigInteger.
    [Benchmark]
    public int ReadHugeIntsAsObject()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT h1, h2, h3, h4 FROM typed";
        using var reader = command.ExecuteReader();

        var hash = 0;
        while (reader.Read())
        {
            for (var i = 0; i < 4; i++)
            {
                hash ^= reader.GetValue(i).GetHashCode();
            }
        }

        return hash;
    }

    [Benchmark]
    public int ReadHugeIntsAsBigInteger()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT h1, h2, h3, h4 FROM typed";
        using var reader = command.ExecuteReader();

        var sign = 0;
        while (reader.Read())
        {
            for (var i = 0; i < 4; i++)
            {
                sign += reader.GetFieldValue<BigInteger>(i).Sign;
            }
        }

        return sign;
    }

    // Values that need more than 64 bits. A BigInteger then has to allocate, whichever way it is made.
    [Benchmark]
    public int ReadLargeHugeIntsAsBigInteger()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT b1, b2, b3, b4 FROM typed";
        using var reader = command.ExecuteReader();

        var sign = 0;
        while (reader.Read())
        {
            for (var i = 0; i < 4; i++)
            {
                sign += reader.GetFieldValue<BigInteger>(i).Sign;
            }
        }

        return sign;
    }

    // A DECIMAL of more than 18 digits is stored as a HUGEINT.
    [Benchmark]
    public decimal ReadWideDecimals()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT w1, w2, w3, w4 FROM typed";
        using var reader = command.ExecuteReader();

        var sum = 0m;
        while (reader.Read())
        {
            sum += reader.GetDecimal(0) + reader.GetDecimal(1) + reader.GetDecimal(2) + reader.GetDecimal(3);
        }

        return sum;
    }

    [Benchmark]
    public int ReadVarIntsAsBigInteger()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT v1, v2 FROM typed";
        using var reader = command.ExecuteReader();

        var sign = 0;
        while (reader.Read())
        {
            sign += reader.GetFieldValue<BigInteger>(0).Sign + reader.GetFieldValue<BigInteger>(1).Sign;
        }

        return sign;
    }

    [Benchmark]
    public long ReadHugeIntsAsLong()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT h1, h2, h3, h4 FROM typed";
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetInt64(0) + reader.GetInt64(1) + reader.GetInt64(2) + reader.GetInt64(3);
        }

        return sum;
    }
}
