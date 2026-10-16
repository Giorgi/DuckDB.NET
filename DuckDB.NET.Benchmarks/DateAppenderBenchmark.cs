using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

// Four columns of one date or time type per benchmark. Up to 1.5.6-alpha.28, every date and timestamp appended
// allocated several hundred bytes, so the Allocated column matters here as much as the time.
// IDuckDBAppenderRow has no DateOnly or TimeOnly overload (one is planned for 2.0), so AppendDateOnlyToDate and
// AppendTimeOnlyToTime write through the implicit conversions to DuckDBDateOnly and DuckDBTimeOnly.
[MemoryDiagnoser]
public class DateAppenderBenchmark
{
    private static readonly DateTime Start = new(2024, 1, 15, 12, 30, 45);

    private DuckDBConnection connection = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);
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

    [IterationSetup(Target = nameof(AppendDateTimeToTimestamp))]
    public void TimestampSetup() => CreateTable("TIMESTAMP");

    [IterationSetup(Target = nameof(AppendDateTimeOffsetToTimestampTz))]
    public void TimestampTzSetup() => CreateTable("TIMESTAMPTZ");

    [IterationSetup(Targets = [nameof(AppendDateTimeToDate), nameof(AppendDateOnlyToDate)])]
    public void DateSetup() => CreateTable("DATE");

    [IterationSetup(Target = nameof(AppendTimeOnlyToTime))]
    public void TimeSetup() => CreateTable("TIME");

    [Benchmark]
    public void AppendDateTimeToTimestamp()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((DateTime?)Start.AddSeconds(i), static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    [Benchmark]
    public void AppendDateTimeOffsetToTimestampTz()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((DateTimeOffset?)new DateTimeOffset(Start.AddSeconds(i), TimeSpan.Zero), static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    [Benchmark]
    public void AppendDateTimeToDate()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((DateTime?)Start.AddDays(i % 1000), static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    [Benchmark]
    public void AppendDateOnlyToDate()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((DateOnly?)DateOnly.FromDateTime(Start.AddDays(i % 1000)), static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }

    // A time has no infinity check, so it never allocated. It is here as the reference for the others.
    [Benchmark]
    public void AppendTimeOnlyToTime()
    {
        using var appender = connection.CreateAppender("bench");

        for (var i = 0; i < RowCount; i++)
        {
            appender.AppendRow((TimeOnly?)TimeOnly.FromDateTime(Start.AddSeconds(i % 3600)), static (row, value) =>
            {
                row.AppendValue(value).AppendValue(value).AppendValue(value).AppendValue(value);
            });
        }
    }
}
