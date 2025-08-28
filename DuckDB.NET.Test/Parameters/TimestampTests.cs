using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using FluentAssertions.Extensions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class TimestampTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_500)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700)]
    public void QueryScalarTest(int year, int mon, int day, int hour, int minute, int second, int microsecond)
    {
        var expectedValue = new DateTime(year, mon, day, hour, minute, second).AddTicks(microsecond * 10);

        Command.CommandText = $"SELECT TIMESTAMP '{year}-{mon}-{day} {hour}:{minute}:{second}.{microsecond:000000}';";

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var receivedTime = (DateTime)scalar;

        receivedTime.Year.Should().Be(year);
        receivedTime.Month.Should().Be(mon);
        receivedTime.Day.Should().Be(day);
        receivedTime.Hour.Should().Be(hour);
        receivedTime.Minute.Should().Be(minute);
        receivedTime.Second.Should().Be(second);
        receivedTime.Millisecond.Should().Be(microsecond / 1000);

        receivedTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);
    }

    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_500)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700)]
    public void BindTest(int year, int mon, int day, int hour, int minute, int second, int microsecond)
    {
        var expectedValue = new DateTime(year, mon, day, hour, minute, second).AddTicks(microsecond * 10);

        Command.CommandText = "SELECT ?::TimeStamp;";
        Command.Parameters.Add(new DuckDBParameter(expectedValue));

        var scalar = Command.ExecuteScalar();

        scalar.Should().BeOfType<DateTime>();

        var receivedTime = (DateTime)scalar;

        receivedTime.Year.Should().Be(year);
        receivedTime.Month.Should().Be(mon);
        receivedTime.Day.Should().Be(day);
        receivedTime.Hour.Should().Be(hour);
        receivedTime.Minute.Should().Be(minute);
        receivedTime.Second.Should().Be(second);
        receivedTime.Millisecond.Should().Be(microsecond / 1000);

        receivedTime.TimeOfDay.Should().Be(expectedValue.TimeOfDay);
    }

    [Theory]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_000, 0)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_000, 0)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_000, 0)]
    [InlineData(1992, 09, 20, 12, 15, 17, 350_300, 0)]
    [InlineData(2022, 05, 04, 12, 17, 15, 450_543, 200)]
    [InlineData(2022, 04, 05, 18, 15, 17, 125_700, 0)]
    public void InsertAndQueryTest(int year, int mon, int day, byte hour, byte minute, byte second, int microsecond, int nanosecond)
    {
        var expectedValue = new DateTime(year, mon, day, hour, minute, second, DateTimeKind.Utc).AddTicks(microsecond * 10).AddNanoseconds(nanosecond);

        TestTimestampInsert("TIMESTAMP", DuckDBType.Timestamp, expectedValue);

        TestTimestampInsert("TIMESTAMP_S", DuckDBType.TimestampS, expectedValue);

        TestTimestampInsert("TIMESTAMP_MS", DuckDBType.TimestampMs, expectedValue);

        TestTimestampInsert("TIMESTAMP_NS", DuckDBType.TimestampNs, expectedValue);

        TestTimestampInsert("TIMESTAMPTZ", DuckDBType.TimestampTz, expectedValue);
    }

    private void TestTimestampInsert(string timestampType, DuckDBType duckDBType, DateTime expectedValue)
    {
        expectedValue = duckDBType switch
        {
            DuckDBType.Timestamp or DuckDBType.TimestampTz => Trim(expectedValue, TimeSpan.TicksPerMicrosecond),
            DuckDBType.TimestampS => Trim(expectedValue, TimeSpan.TicksPerSecond),
            DuckDBType.TimestampMs => Trim(expectedValue, TimeSpan.TicksPerMillisecond),
            DuckDBType.TimestampNs => Trim(expectedValue, TimeSpan.FromTicks(100).Ticks),
            _ => expectedValue
        };

        Command.CommandText = $"CREATE OR Replace TABLE TimestampTestTable (a INTEGER, b {timestampType});";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO TimestampTestTable (a, b) VALUES (42, ?);";
        Command.Parameters.Add(new DuckDBParameter(expectedValue));
        Command.ExecuteNonQuery();

        Command.Parameters.Clear();
        Command.CommandText = "SELECT * FROM TimestampTestTable LIMIT 1;";

        var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldType(1).Should().Be(typeof(DateTime));

        var databaseValue = reader.GetDateTime(1);

        databaseValue.Year.Should().Be(expectedValue.Year);
        databaseValue.Month.Should().Be(expectedValue.Month);
        databaseValue.Day.Should().Be(expectedValue.Day);
        databaseValue.Hour.Should().Be(expectedValue.Hour);
        databaseValue.Minute.Should().Be(expectedValue.Minute);
        databaseValue.Second.Should().Be(expectedValue.Second);

        databaseValue.Millisecond.Should().Be(expectedValue.Millisecond);
        databaseValue.Microsecond.Should().Be(expectedValue.Microsecond);
        databaseValue.Nanosecond.Should().Be(expectedValue.Nanosecond);

        databaseValue.TimeOfDay.Should().Be(expectedValue.TimeOfDay);

        var dateTimeNullable = reader.GetFieldValue<DateTime?>(1);
        databaseValue = dateTimeNullable.Value;

        databaseValue.Year.Should().Be(expectedValue.Year);
        databaseValue.Month.Should().Be(expectedValue.Month);
        databaseValue.Day.Should().Be(expectedValue.Day);
        databaseValue.Hour.Should().Be(expectedValue.Hour);
        databaseValue.Minute.Should().Be(expectedValue.Minute);
        databaseValue.Second.Should().Be(expectedValue.Second);

        databaseValue.Millisecond.Should().Be(expectedValue.Millisecond);
        databaseValue.Microsecond.Should().Be(expectedValue.Microsecond);
        databaseValue.Nanosecond.Should().Be(expectedValue.Nanosecond);

        databaseValue.TimeOfDay.Should().Be(expectedValue.TimeOfDay);

        var dateTimeOffset = reader.GetFieldValue<DateTimeOffset>(1);

        dateTimeOffset.Year.Should().Be(expectedValue.Year);
        dateTimeOffset.Month.Should().Be(expectedValue.Month);
        dateTimeOffset.Day.Should().Be(expectedValue.Day);
        dateTimeOffset.Hour.Should().Be(expectedValue.Hour);
        dateTimeOffset.Minute.Should().Be(expectedValue.Minute);
        dateTimeOffset.Second.Should().Be(expectedValue.Second);

        dateTimeOffset.Millisecond.Should().Be(expectedValue.Millisecond);
        dateTimeOffset.Microsecond.Should().Be(expectedValue.Microsecond);
        dateTimeOffset.Nanosecond.Should().Be(expectedValue.Nanosecond);

        if (duckDBType == DuckDBType.TimestampTz)
        {
            Command.CommandText = "Truncate table TimestampTestTable";
            Command.ExecuteNonQuery();

            Command.CommandText = "INSERT INTO TimestampTestTable (a, b) VALUES (42, ?);";
            Command.Parameters.Add(new DuckDBParameter(new DateTimeOffset(expectedValue)));
            Command.ExecuteNonQuery();

            Command.Parameters.Clear();
            Command.CommandText = "SELECT * FROM TimestampTestTable LIMIT 1;";

            reader = Command.ExecuteReader();
            reader.Read();

            dateTimeOffset = reader.GetFieldValue<DateTimeOffset>(1);

            dateTimeOffset.Year.Should().Be(expectedValue.Year);
            dateTimeOffset.Month.Should().Be(expectedValue.Month);
            dateTimeOffset.Day.Should().Be(expectedValue.Day);
            dateTimeOffset.Hour.Should().Be(expectedValue.Hour);
            dateTimeOffset.Minute.Should().Be(expectedValue.Minute);
            dateTimeOffset.Second.Should().Be(expectedValue.Second);

            dateTimeOffset.Millisecond.Should().Be(expectedValue.Millisecond);
            dateTimeOffset.Microsecond.Should().Be(expectedValue.Microsecond);
            dateTimeOffset.Nanosecond.Should().Be(expectedValue.Nanosecond);
        }
    }

    [Fact]
    public void BindParameterWithoutTable_DateTime()
    {
        var value = Trim(Faker.Date.Recent(), TimeSpan.FromTicks(100).Ticks);
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<DateTime>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_DateTimeOffset()
    {
        var value = Trim(Faker.Date.PastOffset(), TimeSpan.FromTicks(100).Ticks);
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        using var reader = Command.ExecuteReader();
        reader.Read();
        var result = reader.GetFieldValue<DateTimeOffset>(0);
        
        result.Should().Be(value);
    }

    public static TimeOnly Trim(TimeOnly date, long ticks) => new(date.Ticks - date.Ticks % ticks);

    public static DateTime Trim(DateTime date, long ticks) => new(date.Ticks - date.Ticks % ticks, date.Kind);

    public static DateTimeOffset Trim(DateTimeOffset date, long ticks) => new(Trim(date.DateTime, ticks), date.Offset);
}