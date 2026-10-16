using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Reads of the date and time types, four columns of each so that a read is a large part of the time per row.
[MemoryDiagnoser]
public class DateTimeReaderBenchmark
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
        command.CommandText = "CREATE TABLE times AS SELECT " +
                              Columns("ts", "TIMESTAMP '2020-01-01 10:20:30.123456' + INTERVAL ({0}) SECOND") + ", " +
                              Columns("ns", "(TIMESTAMP '2020-01-01 10:20:30.123456' + INTERVAL ({0}) SECOND)::TIMESTAMP_NS") + ", " +
                              Columns("tz", "(TIMESTAMP '2020-01-01 10:20:30.123456' + INTERVAL ({0}) SECOND)::TIMESTAMPTZ") + ", " +
                              Columns("d", "DATE '1990-01-01' + ({0} % 20000)::INTEGER") + ", " +
                              Columns("t", "TIME '00:00:00.123456' + INTERVAL ({0} % 86000) SECOND") + ", " +
                              Columns("tt", "(TIME '00:00:00.123456' + INTERVAL ({0} % 86000) SECOND)::TIMETZ") +
                              $" FROM range({RowCount}) t(i);";
        command.ExecuteNonQuery();

        static string Columns(string name, string expression)
        {
            return string.Join(", ", Enumerable.Range(1, 4).Select(n => string.Format(expression, $"i + {n}") + $" {name}{n}"));
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [Benchmark]
    public long ReadTimestampsAsDateTime()
    {
        using var reader = Read("ts");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetDateTime(0).Ticks + reader.GetDateTime(1).Ticks + reader.GetDateTime(2).Ticks + reader.GetDateTime(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimestampNsAsDateTime()
    {
        using var reader = Read("ns");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetDateTime(0).Ticks + reader.GetDateTime(1).Ticks + reader.GetDateTime(2).Ticks + reader.GetDateTime(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimestampTzAsDateTimeOffset()
    {
        using var reader = Read("tz");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetFieldValue<DateTimeOffset>(0).Ticks + reader.GetFieldValue<DateTimeOffset>(1).Ticks +
                   reader.GetFieldValue<DateTimeOffset>(2).Ticks + reader.GetFieldValue<DateTimeOffset>(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimestampsWithGetValue()
    {
        using var reader = Read("ts");

        var sum = 0L;
        while (reader.Read())
        {
            sum += ((DateTime)reader.GetValue(0)).Ticks + ((DateTime)reader.GetValue(1)).Ticks + ((DateTime)reader.GetValue(2)).Ticks + ((DateTime)reader.GetValue(3)).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadDatesAsDateTime()
    {
        using var reader = Read("d");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetDateTime(0).Ticks + reader.GetDateTime(1).Ticks + reader.GetDateTime(2).Ticks + reader.GetDateTime(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadDatesAsDateOnly()
    {
        using var reader = Read("d");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetFieldValue<DateOnly>(0).DayNumber + reader.GetFieldValue<DateOnly>(1).DayNumber +
                   reader.GetFieldValue<DateOnly>(2).DayNumber + reader.GetFieldValue<DateOnly>(3).DayNumber;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimesAsTimeOnly()
    {
        using var reader = Read("t");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetFieldValue<TimeOnly>(0).Ticks + reader.GetFieldValue<TimeOnly>(1).Ticks +
                   reader.GetFieldValue<TimeOnly>(2).Ticks + reader.GetFieldValue<TimeOnly>(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimesAsDateTime()
    {
        using var reader = Read("t");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetDateTime(0).Ticks + reader.GetDateTime(1).Ticks + reader.GetDateTime(2).Ticks + reader.GetDateTime(3).Ticks;
        }

        return sum;
    }

    [Benchmark]
    public long ReadTimeTzAsDateTimeOffset()
    {
        using var reader = Read("tt");

        var sum = 0L;
        while (reader.Read())
        {
            sum += reader.GetFieldValue<DateTimeOffset>(0).Ticks + reader.GetFieldValue<DateTimeOffset>(1).Ticks +
                   reader.GetFieldValue<DateTimeOffset>(2).Ticks + reader.GetFieldValue<DateTimeOffset>(3).Ticks;
        }

        return sum;
    }

    private DuckDBDataReader Read(string name)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {name}1, {name}2, {name}3, {name}4 FROM times";

        return command.ExecuteReader();
    }
}
