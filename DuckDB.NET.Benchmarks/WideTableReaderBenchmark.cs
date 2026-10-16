using System.Text;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// A table of mixed column types, read three ways: the typed getters by ordinal, the typed getters by column name
// (GetOrdinal on every row, as hand-written code often does), and GetValues into an object array, as Dapper and
// DataTable.Load do. The columns repeat the seven types in order, so 5 columns have no BIGINT and no TIMESTAMP,
// and 20 have every type about three times.
[MemoryDiagnoser]
public class WideTableReaderBenchmark
{
    private enum Kind { Integer, Varchar, Double, Boolean, Decimal, BigInt, Timestamp }

    private static readonly Kind[] Kinds = [Kind.Integer, Kind.Varchar, Kind.Double, Kind.Boolean, Kind.Decimal, Kind.BigInt, Kind.Timestamp];

    private DuckDBConnection connection = null!;
    private Kind[] columns = null!;
    private string[] names = null!;

    [Params(100_000)]
    public int RowCount { get; set; }

    [Params(5, 10, 20)]
    public int ColumnCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        columns = new Kind[ColumnCount];
        names = new string[ColumnCount];
        var select = new StringBuilder();
        for (var i = 0; i < ColumnCount; i++)
        {
            columns[i] = Kinds[i % Kinds.Length];
            names[i] = $"c{i}";
            if (i > 0) select.Append(", ");

            select.Append(columns[i] switch
            {
                Kind.Integer => $"i::INTEGER c{i}",
                Kind.Varchar => $"'val_' || i::VARCHAR c{i}",
                Kind.Double => $"(i * 0.1)::DOUBLE c{i}",
                Kind.Boolean => $"(i % 2 = 0) c{i}",
                Kind.Decimal => $"(i * 1.23)::DECIMAL(18,2) c{i}",
                Kind.BigInt => $"i::BIGINT * 100000 c{i}",
                _ => $"TIMESTAMP '2024-01-01' + to_seconds(i) c{i}",
            });
        }

        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TABLE wide AS SELECT {select} FROM range({RowCount}) t(i)";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public long ReadAllColumns()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM wide";
        using var reader = command.ExecuteReader();

        var hash = 0L;
        while (reader.Read())
        {
            for (var i = 0; i < columns.Length; i++)
            {
                hash += Read(reader, i);
            }
        }

        return hash;
    }

    [Benchmark]
    public long ReadAllColumnsByName()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM wide";
        using var reader = command.ExecuteReader();

        var hash = 0L;
        while (reader.Read())
        {
            for (var i = 0; i < names.Length; i++)
            {
                hash += Read(reader, reader.GetOrdinal(names[i]));
            }
        }

        return hash;
    }

    [Benchmark]
    public long ReadAllColumnsWithGetValues()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM wide";
        using var reader = command.ExecuteReader();

        var values = new object[columns.Length];
        var hash = 0L;
        while (reader.Read())
        {
            reader.GetValues(values);
            hash += values[0].GetHashCode();
        }

        return hash;
    }

    // Every value goes into the result, so that the JIT cannot leave a read out.
    private long Read(DuckDBDataReader reader, int ordinal) => columns[ordinal] switch
    {
        Kind.Integer => reader.GetInt32(ordinal),
        Kind.Varchar => reader.GetString(ordinal).Length,
        Kind.Double => (long)reader.GetDouble(ordinal),
        Kind.Boolean => reader.GetBoolean(ordinal) ? 1 : 0,
        Kind.Decimal => (long)reader.GetDecimal(ordinal),
        Kind.BigInt => reader.GetInt64(ordinal),
        _ => reader.GetDateTime(ordinal).Ticks,
    };
}
