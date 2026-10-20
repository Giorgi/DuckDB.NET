using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// ReadRowsOnly is the cost of running the query and moving through the rows, and the control of the readers for the
// whole run. What the other benchmarks add on top of it is the cost of the getters. The n columns have a NULL in
// every tenth row, each at another row.
[MemoryDiagnoser]
public class DataReaderBenchmark
{
    private const string IntegerColumns = "SELECT a, b, c, d, e, f, g, h FROM bench";
    private const string NullableColumns = "SELECT n1, n2, n3, n4, n5, n6, n7, n8 FROM bench";

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
        command.CommandText = "CREATE TABLE bench AS SELECT i::INTEGER a, i::INTEGER b, i::INTEGER c, i::INTEGER d, i::INTEGER e, i::INTEGER f, i::INTEGER g, i::INTEGER h, " +
                              "i % 2 = 0 ba, i % 3 = 0 bb, i % 5 = 0 bc, i % 7 = 0 bd, i % 2 = 1 be, i % 3 = 1 bf, i % 5 = 1 bg, i % 7 = 1 bh, " +
                              "CASE WHEN i % 10 = 0 THEN NULL ELSE i END::INTEGER n1, CASE WHEN i % 10 = 3 THEN NULL ELSE i END::INTEGER n2, " +
                              "CASE WHEN i % 10 = 6 THEN NULL ELSE i END::INTEGER n3, CASE WHEN i % 10 = 9 THEN NULL ELSE i END::INTEGER n4, " +
                              "CASE WHEN i % 10 = 1 THEN NULL ELSE i END::INTEGER n5, CASE WHEN i % 10 = 4 THEN NULL ELSE i END::INTEGER n6, " +
                              "CASE WHEN i % 10 = 7 THEN NULL ELSE i END::INTEGER n7, CASE WHEN i % 10 = 2 THEN NULL ELSE i END::INTEGER n8 " +
                              $"FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public int ReadRowsOnly()
    {
        using var command = connection.CreateCommand();
        command.CommandText = IntegerColumns;
        using var reader = command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
        }

        return rows;
    }

    [Benchmark]
    public long ReadIntegers()
    {
        using var command = connection.CreateCommand();
        command.CommandText = IntegerColumns;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetInt32(0) + reader.GetInt32(1) + reader.GetInt32(2) + reader.GetInt32(3) +
                   reader.GetInt32(4) + reader.GetInt32(5) + reader.GetInt32(6) + reader.GetInt32(7);
        }

        return sum;
    }

    // The object path, which Dapper uses.
    [Benchmark]
    public int ReadIntegersAsObject()
    {
        using var command = connection.CreateCommand();
        command.CommandText = IntegerColumns;
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

    // INTEGER read as long: a conversion that cannot overflow.
    [Benchmark]
    public long ReadIntegersAsLong()
    {
        using var command = connection.CreateCommand();
        command.CommandText = IntegerColumns;
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
    public int ReadBooleans()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT ba, bb, bc, bd, be, bf, bg, bh FROM bench";
        using var reader = command.ExecuteReader();

        var count = 0;
        while (reader.Read())
        {
            for (var i = 0; i < 8; i++)
            {
                if (reader.GetBoolean(i))
                {
                    count++;
                }
            }
        }

        return count;
    }


    // Columns with NULLs, read the two ways an application does: GetFieldValue of a nullable type, and IsDBNull
    // before the typed getter, which is what Dapper and Entity Framework do. Eight columns, as in ReadIntegers, so
    // that the times of the two can be compared.
    [Benchmark]
    public long ReadNullableIntegers()
    {
        using var command = connection.CreateCommand();
        command.CommandText = NullableColumns;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            for (var i = 0; i < 8; i++)
            {
                sum += reader.GetFieldValue<int?>(i) ?? 0;
            }
        }

        return sum;
    }

    [Benchmark]
    public long ReadIntegersCheckingNull()
    {
        using var command = connection.CreateCommand();
        command.CommandText = NullableColumns;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            for (var i = 0; i < 8; i++)
            {
                if (!reader.IsDBNull(i))
                {
                    sum += reader.GetInt32(i);
                }
            }
        }

        return sum;
    }

    // ReadIntegers with UseStreamingMode: the chunks are fetched while the rows are read instead of with the query.
    [Benchmark]
    public long ReadIntegersStreaming()
    {
        using var command = connection.CreateCommand();
        command.CommandText = IntegerColumns;
        command.UseStreamingMode = true;
        using var reader = command.ExecuteReader();

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetInt32(0) + reader.GetInt32(1) + reader.GetInt32(2) + reader.GetInt32(3) +
                   reader.GetInt32(4) + reader.GetInt32(5) + reader.GetInt32(6) + reader.GetInt32(7);
        }

        return sum;
    }
}
