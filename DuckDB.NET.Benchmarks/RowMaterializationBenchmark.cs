using System.Data;
using BenchmarkDotNet.Attributes;
using Dapper;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// A result materialized into a List<T> of row objects, the four ways an application gets there: a loop over the
// typed getters; a loop over the indexer with a DBNull test and a cast, which is what Dapper's deserializer emits
// for a member type without its UseGetFieldValue flag; Dapper as shipped; and Dapper with every member type
// registered to read through GetFieldValue<T>. RowsOnly runs the query and moves through the rows without reading
// a value: the control that the others add the materialization to.
//
// Two tables of eight columns, so that the value count is the same: "mixed" has numbers, a timestamp and two
// strings of distinct values, where the indexer path boxes; "strings" is eight VARCHAR columns whose values repeat
// every hundred rows, the shape of a customer table, where nothing boxes.
[MemoryDiagnoser]
public class RowMaterializationBenchmark
{
    private const string MixedColumns = "SELECT id, quantity, total, ratio, amount, created, code, name FROM mixed";
    private const string StringColumns = "SELECT s1, s2, s3, s4, s5, s6, s7, s8 FROM strings";

    // Dapper caches a deserializer per query text, so the GetFieldValue variants query with a text of their own.
    private const string GetFieldValueSuffix = " -- GetFieldValue";

    private DuckDBConnection connection = null!;

    [Params(500_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE mixed AS SELECT " +
                              "i::INTEGER AS id, " +
                              "(i % 1000)::INTEGER AS quantity, " +
                              "(i * 1000)::BIGINT AS total, " +
                              "(i / 7.0)::DOUBLE AS ratio, " +
                              "(i / 100.0)::DECIMAL(18, 3) AS amount, " +
                              "TIMESTAMP '2024-01-01' + to_seconds(i) AS created, " +
                              "'C' || (i % 1000)::VARCHAR AS code, " +    // 2 to 4 bytes, inline
                              "'customer-' || md5(i::VARCHAR) AS name " + // 41 bytes
                              $"FROM range({RowCount}) t(i); " +
                              "CREATE TABLE strings AS SELECT " +
                              "(i % 100)::VARCHAR AS s1, " +                    // 1 to 2 bytes, inline
                              "'customer-' || (i % 100)::VARCHAR AS s2, " +     // 10 to 11 bytes, inline
                              "md5((i % 100)::VARCHAR) AS s3, " +               // 32 bytes
                              "'Company ' || md5((i % 100)::VARCHAR) AS s4, " + // 40 bytes
                              "CASE i % 7 WHEN 0 THEN 'Berlin' WHEN 1 THEN 'London' WHEN 2 THEN 'Tbilisi' WHEN 3 THEN 'Mexico City' " +
                              "WHEN 4 THEN 'Paris' WHEN 5 THEN 'Madrid' ELSE 'Seattle' END AS s5, " + // 5 to 11 bytes, inline
                              "CASE i % 5 WHEN 0 THEN 'Germany' WHEN 1 THEN 'UK' WHEN 2 THEN 'Georgia' WHEN 3 THEN 'Mexico' ELSE 'France' END AS s6, " + // inline
                              "'(' || (i % 100)::VARCHAR || ') 555-' || (1000 + i % 100)::VARCHAR AS s7, " + // 12 to 13 bytes, at the inline limit
                              "CASE i % 3 WHEN 0 THEN 'Owner' WHEN 1 THEN 'Sales Manager' ELSE 'Sales Representative' END AS s8 " + // 5 to 20 bytes
                              $"FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    public sealed class MixedRow
    {
        public int Id { get; set; }
        public int Quantity { get; set; }
        public long Total { get; set; }
        public double Ratio { get; set; }
        public decimal Amount { get; set; }
        public DateTime Created { get; set; }
        public string Code { get; set; } = null!;
        public string Name { get; set; } = null!;
    }

    public sealed class StringRow
    {
        public string S1 { get; set; } = null!;
        public string S2 { get; set; } = null!;
        public string S3 { get; set; } = null!;
        public string S4 { get; set; } = null!;
        public string S5 { get; set; } = null!;
        public string S6 { get; set; } = null!;
        public string S7 { get; set; } = null!;
        public string S8 { get; set; } = null!;
    }

    [Benchmark]
    public int MixedRowsOnly() => CountRows(MixedColumns);

    [Benchmark]
    public List<MixedRow> MixedTyped()
    {
        using var command = connection.CreateCommand();
        command.CommandText = MixedColumns;
        using var reader = command.ExecuteReader();

        var rows = new List<MixedRow>();
        while (reader.Read())
        {
            rows.Add(new MixedRow
            {
                Id = reader.GetInt32(0),
                Quantity = reader.GetInt32(1),
                Total = reader.GetInt64(2),
                Ratio = reader.GetDouble(3),
                Amount = reader.GetDecimal(4),
                Created = reader.GetDateTime(5),
                Code = reader.GetString(6),
                Name = reader.GetString(7),
            });
        }

        return rows;
    }

    // Dapper's path written by hand, so that Dapper's own per-row work can be told apart from the reader's object
    // path: the indexer, a DBNull test, an unbox or a cast. A DBNull leaves the member at its default, as in Dapper.
    [Benchmark]
    public List<MixedRow> MixedGetValue()
    {
        using var command = connection.CreateCommand();
        command.CommandText = MixedColumns;
        using var reader = command.ExecuteReader();

        var rows = new List<MixedRow>();
        while (reader.Read())
        {
            var row = new MixedRow();

            var value = reader[0];
            if (value is not DBNull) row.Id = (int)value;
            value = reader[1];
            if (value is not DBNull) row.Quantity = (int)value;
            value = reader[2];
            if (value is not DBNull) row.Total = (long)value;
            value = reader[3];
            if (value is not DBNull) row.Ratio = (double)value;
            value = reader[4];
            if (value is not DBNull) row.Amount = (decimal)value;
            value = reader[5];
            if (value is not DBNull) row.Created = (DateTime)value;
            value = reader[6];
            if (value is not DBNull) row.Code = (string)value;
            value = reader[7];
            if (value is not DBNull) row.Name = (string)value;

            rows.Add(row);
        }

        return rows;
    }

    [Benchmark]
    public List<MixedRow> MixedDapper() => connection.Query<MixedRow>(MixedColumns).AsList();

    [Benchmark]
    public List<MixedRow> MixedDapperGetFieldValue()
    {
        MapTypesToGetFieldValue();
        return connection.Query<MixedRow>(MixedColumns + GetFieldValueSuffix).AsList();
    }

    [Benchmark]
    public int StringsRowsOnly() => CountRows(StringColumns);

    [Benchmark]
    public List<StringRow> StringsTyped()
    {
        using var command = connection.CreateCommand();
        command.CommandText = StringColumns;
        using var reader = command.ExecuteReader();

        var rows = new List<StringRow>();
        while (reader.Read())
        {
            rows.Add(new StringRow
            {
                S1 = reader.GetString(0),
                S2 = reader.GetString(1),
                S3 = reader.GetString(2),
                S4 = reader.GetString(3),
                S5 = reader.GetString(4),
                S6 = reader.GetString(5),
                S7 = reader.GetString(6),
                S8 = reader.GetString(7),
            });
        }

        return rows;
    }

    [Benchmark]
    public List<StringRow> StringsGetValue()
    {
        using var command = connection.CreateCommand();
        command.CommandText = StringColumns;
        using var reader = command.ExecuteReader();

        var rows = new List<StringRow>();
        while (reader.Read())
        {
            var row = new StringRow();

            var value = reader[0];
            if (value is not DBNull) row.S1 = (string)value;
            value = reader[1];
            if (value is not DBNull) row.S2 = (string)value;
            value = reader[2];
            if (value is not DBNull) row.S3 = (string)value;
            value = reader[3];
            if (value is not DBNull) row.S4 = (string)value;
            value = reader[4];
            if (value is not DBNull) row.S5 = (string)value;
            value = reader[5];
            if (value is not DBNull) row.S6 = (string)value;
            value = reader[6];
            if (value is not DBNull) row.S7 = (string)value;
            value = reader[7];
            if (value is not DBNull) row.S8 = (string)value;

            rows.Add(row);
        }

        return rows;
    }

    [Benchmark]
    public List<StringRow> StringsDapper() => connection.Query<StringRow>(StringColumns).AsList();

    [Benchmark]
    public List<StringRow> StringsDapperGetFieldValue()
    {
        MapTypesToGetFieldValue();
        return connection.Query<StringRow>(StringColumns + GetFieldValueSuffix).AsList();
    }

    private int CountRows(string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
        }

        return rows;
    }

    // Dapper's type map is static, and a type registered with useGetFieldValue is read with GetFieldValue<T>
    // instead of the indexer. Out of the box only SqlDecimal, SqlMoney, DateOnly and TimeOnly are. The registration
    // is made here and not in the setup because BenchmarkDotNet runs every benchmark in a process of its own, so
    // the plain Dapper benchmarks never see it.
    private static bool mappedToGetFieldValue;

    private static void MapTypesToGetFieldValue()
    {
        if (mappedToGetFieldValue)
        {
            return;
        }

        SqlMapper.AddTypeMap(typeof(int), DbType.Int32, useGetFieldValue: true);
        SqlMapper.AddTypeMap(typeof(long), DbType.Int64, useGetFieldValue: true);
        SqlMapper.AddTypeMap(typeof(double), DbType.Double, useGetFieldValue: true);
        SqlMapper.AddTypeMap(typeof(decimal), DbType.Decimal, useGetFieldValue: true);
        SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime, useGetFieldValue: true);
        SqlMapper.AddTypeMap(typeof(string), DbType.String, useGetFieldValue: true);

        mappedToGetFieldValue = true;
    }
}
