using System.Globalization;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// DuckDB.NET converts a DateTime, a DateTimeOffset, a DateOnly and a TimeOnly to what DuckDB stores with its own
// arithmetic, without asking DuckDB. TemporalConversionTests guards the read; these tests guard the write.
// They compare the arithmetic with DuckDB's own functions for the same value (duckdb_to_date, duckdb_to_timestamp,
// duckdb_to_time, duckdb_create_time_tz), and what was written with the parts DuckDB splits it into again.
// If a DuckDB release changes how these types are stored or what they mean, these tests fail.
// Do not replace the comparisons with constants worked out in C#.
public class TemporalWriteConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void EveryDateIsStoredAsDuckDBStoresIt()
    {
        var lastTickOfTheDay = new TimeOnly(TimeSpan.TicksPerDay - 1);

        for (var dayNumber = 0; dayNumber <= DateOnly.MaxValue.DayNumber; dayNumber++)
        {
            var date = DateOnly.FromDayNumber(dayNumber);
            var expected = NativeMethods.DateTimeHelpers.DuckDBToDate(new DuckDBDateOnly(date.Year, (byte)date.Month, (byte)date.Day)).Days;

            // Compared without FluentAssertions: there are 3.6 million days.
            if (date.ToDuckDBDate().Days != expected ||
                date.ToDateTime(TimeOnly.MinValue).ToDuckDBDate().Days != expected ||
                date.ToDateTime(lastTickOfTheDay).ToDuckDBDate().Days != expected)
            {
                Assert.Fail($"{date:O}: DuckDB stores {expected}, as DateOnly it became {date.ToDuckDBDate().Days}, as DateTime {date.ToDateTime(TimeOnly.MinValue).ToDuckDBDate().Days} and {date.ToDateTime(lastTickOfTheDay).ToDuckDBDate().Days}");
            }
        }
    }

    [Fact]
    public void TimestampsAreStoredAsDuckDBStoresThem()
    {
        TimeSpan[] offsets = [TimeSpan.Zero, TimeSpan.FromHours(14), TimeSpan.FromHours(-14), TimeSpan.FromMinutes(330), TimeSpan.FromMinutes(-1)];

        var index = 0;
        foreach (var ticks in Ticks(200_000))
        {
            var dateTime = new DateTime(ticks, (DateTimeKind)(index % 3));
            var expected = DuckDBMicroseconds(dateTime);

            var offset = offsets[index++ % offsets.Length];
            var utcTicks = ticks - offset.Ticks;
            var withOffset = utcTicks < 0 || utcTicks > DateTime.MaxValue.Ticks ? new DateTimeOffset(ticks, TimeSpan.Zero) : new DateTimeOffset(ticks, offset);

            if (dateTime.ToTimestampStruct(DuckDBType.Timestamp).Micros != expected ||
                dateTime.ToTimestampStruct(DuckDBType.TimestampTz).Micros != expected ||
                withOffset.ToTimestampStruct().Micros != DuckDBMicroseconds(withOffset.UtcDateTime))
            {
                Assert.Fail($"{dateTime:O}: DuckDB stores {expected}, the conversion gave {dateTime.ToTimestampStruct(DuckDBType.Timestamp).Micros}; " +
                            $"{withOffset:O}: DuckDB stores {DuckDBMicroseconds(withOffset.UtcDateTime)}, the conversion gave {withOffset.ToTimestampStruct().Micros}");
            }
        }
    }

    [Fact]
    public void TimesAreStoredAsDuckDBStoresThem()
    {
        var random = new Random(20261012);

        // Every second of the day with a fraction of its own, and the last tick of the day.
        for (var second = 0; second <= 86400; second++)
        {
            var ticks = second == 86400 ? TimeSpan.TicksPerDay - 1 : second * TimeSpan.TicksPerSecond + random.NextInt64(TimeSpan.TicksPerSecond);
            var time = new TimeOnly(ticks);

            var microsecond = (int)(ticks % TimeSpan.TicksPerSecond / 10);
            var expected = NativeMethods.DateTimeHelpers.DuckDBToTime(new DuckDBTimeOnly((byte)time.Hour, (byte)time.Minute, (byte)time.Second, microsecond)).Micros;

            var onADay = new DateTime(2024, 2, 29).AddTicks(ticks);

            // A TIMETZ: the offset moves by 61 minutes from one second to the next, over the whole range of a DateTimeOffset.
            var offset = TimeSpan.FromMinutes((second * 61) % (28 * 60 + 1) - 14 * 60);
            var withOffset = new DateTimeOffset(onADay, offset);
            var expectedTimeTz = NativeMethods.DateTimeHelpers.DuckDBCreateTimeTz(expected, (int)offset.TotalSeconds).Bits;

            if (time.ToDuckDBTime().Micros != expected ||
                onADay.ToDuckDBTime().Micros != expected ||
                withOffset.ToTimeTzStruct().Bits != expectedTimeTz)
            {
                Assert.Fail($"{time:O}: DuckDB stores {expected}, as TimeOnly it became {time.ToDuckDBTime().Micros}, as DateTime {onADay.ToDuckDBTime().Micros}; " +
                            $"with the offset {offset} DuckDB stores {expectedTimeTz:X16}, the conversion gave {withOffset.ToTimeTzStruct().Bits:X16}");
            }
        }
    }

    // The values go through the appender and through parameters, and DuckDB takes them apart again: a read as
    // DuckDBTimestamp, DuckDBDateOnly, DuckDBTimeOnly or DuckDBTimeTz has DuckDB split the stored value, and epoch_us
    // is DuckDB's count for it. A TIMESTAMP_NS is compared with the text DuckDB prints for it, to the nanosecond.
    // The row of the appender takes a DateOnly and a TimeOnly as DuckDBDateOnly and DuckDBTimeOnly, which DuckDB
    // converts. The two lists are there because a list item is converted by the arithmetic.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void WrittenValuesAreSplitByDuckDBIntoTheSameParts(bool throughParameters)
    {
        var table = throughParameters ? "temporal_written_parameters" : "temporal_written_appender";
        Connection.ExecuteNonQuery($"CREATE TABLE {table} (ts TIMESTAMP, tstz TIMESTAMPTZ, tsms TIMESTAMP_MS, tss TIMESTAMP_S, tsns TIMESTAMP_NS, d DATE, d2 DATE, t TIME, ttz TIMETZ, dl DATE[], tl TIME[])");

        // A TIMESTAMP_NS holds 1677-09-21 00:12:43.145224193 to 2262-04-11 23:47:16.854775806.
        var nanosecondMinimum = new DateTime(1677, 9, 22).Ticks;
        var nanosecondMaximum = new DateTime(2262, 4, 11).Ticks;

        var values = Ticks(throughParameters ? 500 : 20_000).Select((ticks, index) =>
        {
            var dateTime = new DateTime(ticks, (DateTimeKind)(index % 3));
            var offset = TimeSpan.FromMinutes((index * 61) % (28 * 60 + 1) - 14 * 60);

            return (DateTime: dateTime,
                    InNanoseconds: new DateTime(nanosecondMinimum + ticks % (nanosecondMaximum - nanosecondMinimum)),
                    // On a day in the middle of the range, so that the offset cannot move it out of the range.
                    WithOffset: new DateTimeOffset(new DateTime(2024, 2, 29).Ticks + ticks % TimeSpan.TicksPerDay, offset));
        }).ToList();

        if (throughParameters)
        {
            using var insert = Connection.CreateCommand();
            insert.CommandText = $"INSERT INTO {table} VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

            foreach (var value in values)
            {
                insert.Parameters.Clear();
                insert.Parameters.Add(new DuckDBParameter(value.DateTime));
                insert.Parameters.Add(new DuckDBParameter(value.WithOffset));
                insert.Parameters.Add(new DuckDBParameter(value.DateTime));
                insert.Parameters.Add(new DuckDBParameter(value.DateTime));
                insert.Parameters.Add(new DuckDBParameter(value.InNanoseconds));
                insert.Parameters.Add(new DuckDBParameter(value.DateTime));
                insert.Parameters.Add(new DuckDBParameter(DateOnly.FromDateTime(value.DateTime)));
                insert.Parameters.Add(new DuckDBParameter(TimeOnly.FromDateTime(value.DateTime)));
                insert.Parameters.Add(new DuckDBParameter(value.WithOffset));
                insert.Parameters.Add(new DuckDBParameter(new List<DateOnly> { DateOnly.FromDateTime(value.DateTime) }));
                insert.Parameters.Add(new DuckDBParameter(new List<TimeOnly> { TimeOnly.FromDateTime(value.DateTime) }));
                insert.ExecuteNonQuery();
            }
        }
        else
        {
            using var appender = Connection.CreateAppender(table);

            foreach (var value in values)
            {
                DateTime? dateTime = value.DateTime;
                DateTime? inNanoseconds = value.InNanoseconds;
                DateTimeOffset? withOffset = value.WithOffset;
                DateOnly? dateOnly = DateOnly.FromDateTime(value.DateTime);
                TimeOnly? timeOnly = TimeOnly.FromDateTime(value.DateTime);

                appender.CreateRow()
                    .AppendValue(dateTime).AppendValue(withOffset).AppendValue(dateTime).AppendValue(dateTime).AppendValue(inNanoseconds)
                    .AppendValue(dateTime).AppendValue(dateOnly).AppendValue(timeOnly).AppendValue(withOffset)
                    .AppendValue(new List<DateOnly> { dateOnly.Value }).AppendValue(new List<TimeOnly> { timeOnly.Value })
                    .EndRow();
            }
        }

        Command.CommandText = $"SELECT ts, epoch_us(tstz), epoch_us(tsms::TIMESTAMP), epoch_us(tss::TIMESTAMP), strftime(tsns, '%Y-%m-%d %H:%M:%S.%n'), d, d2, t, ttz, epoch_us(ts), dl[1], tl[1] FROM {table} ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        foreach (var value in values)
        {
            reader.Read().Should().BeTrue();

            var dateTime = value.DateTime;
            var microsecond = (int)(dateTime.Ticks % TimeSpan.TicksPerSecond / 10);

            var timestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
            var date = reader.GetFieldValue<DuckDBDateOnly>(5);
            var dateOfDateOnly = reader.GetFieldValue<DuckDBDateOnly>(6);
            var time = reader.GetFieldValue<DuckDBTimeOnly>(7);
            var timeTz = reader.GetFieldValue<DuckDBTimeTz>(8);
            var dateInList = reader.GetFieldValue<DuckDBDateOnly>(10);
            var timeInList = reader.GetFieldValue<DuckDBTimeOnly>(11);

            // DuckDB's count of microseconds for the TIMESTAMP column is the reference for the columns in other units.
            var microseconds = reader.GetInt64(9);
            var offsetTicks = value.WithOffset.Ticks % TimeSpan.TicksPerSecond;

            var same =
                (timestamp.Date.Year, timestamp.Date.Month, timestamp.Date.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (timestamp.Time.Hour, timestamp.Time.Min, timestamp.Time.Sec, timestamp.Time.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond) &&
                // The instant: the microseconds of the same moment as a DateTime in UTC, from DuckDB's own function.
                reader.GetInt64(1) == DuckDBMicroseconds(value.WithOffset.UtcDateTime) &&
                // What is below the unit is dropped toward zero, so before 1970 the value moves up.
                reader.GetInt64(2) == microseconds / 1000 * 1000 &&
                reader.GetInt64(3) == microseconds / 1_000_000 * 1_000_000 &&
                // A TIMESTAMP_NS keeps the tick that a TIMESTAMP drops. A tick is 100 nanoseconds: the last two digits are zero.
                reader.GetString(4) == value.InNanoseconds.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture) + "00" &&
                (date.Year, date.Month, date.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (dateOfDateOnly.Year, dateOfDateOnly.Month, dateOfDateOnly.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (time.Hour, time.Min, time.Sec, time.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond) &&
                (dateInList.Year, dateInList.Month, dateInList.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (timeInList.Hour, timeInList.Min, timeInList.Sec, timeInList.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond) &&
                (timeTz.Time.Hour, timeTz.Time.Min, timeTz.Time.Sec, timeTz.Time.Microsecond) == (value.WithOffset.Hour, value.WithOffset.Minute, value.WithOffset.Second, (int)(offsetTicks / 10)) &&
                timeTz.Offset == (int)value.WithOffset.Offset.TotalSeconds;

            if (!same)
            {
                Assert.Fail($"{dateTime:O} and {value.WithOffset:O}: DuckDB has the timestamp as {timestamp.Date.Year}-{timestamp.Date.Month}-{timestamp.Date.Day} " +
                            $"{timestamp.Time.Hour}:{timestamp.Time.Min}:{timestamp.Time.Sec}.{timestamp.Time.Microsecond:D6}, {microseconds} microseconds; " +
                            $"TIMESTAMPTZ {reader.GetInt64(1)}, _MS {reader.GetInt64(2)}, _S {reader.GetInt64(3)}, _NS {reader.GetString(4)}; " +
                            $"the dates as {date.Year}-{date.Month}-{date.Day} and {dateOfDateOnly.Year}-{dateOfDateOnly.Month}-{dateOfDateOnly.Day}; " +
                            $"the time as {time.Hour}:{time.Min}:{time.Sec}.{time.Microsecond:D6}; " +
                            $"in the lists {dateInList.Year}-{dateInList.Month}-{dateInList.Day} and {timeInList.Hour}:{timeInList.Min}:{timeInList.Sec}.{timeInList.Microsecond:D6}; " +
                            $"the TIMETZ as {timeTz.Time.Hour}:{timeTz.Time.Min}:{timeTz.Time.Sec}.{timeTz.Time.Microsecond:D6} with offset {timeTz.Offset}");
            }
        }

        reader.Read().Should().BeFalse();
    }

    // A parameter in "SELECT $1" has no type that DuckDB could report, so the DbType of the parameter picks the
    // conversion. DuckDB splits what it received.
    [Fact]
    public void ParametersWithoutATargetTypeAreSplitByDuckDBIntoTheSameParts()
    {
        Command.CommandText = "SELECT $1";

        var index = 0;
        foreach (var ticks in Ticks(300))
        {
            var dateTime = new DateTime(ticks, (DateTimeKind)(index % 3));
            var microsecond = (int)(ticks % TimeSpan.TicksPerSecond / 10);

            var offset = TimeSpan.FromMinutes((index++ * 61) % (28 * 60 + 1) - 14 * 60);
            var withOffset = new DateTimeOffset(new DateTime(2024, 2, 29).Ticks + ticks % TimeSpan.TicksPerDay, offset);
            var utc = withOffset.UtcDateTime;

            var timestamp = Split<DuckDBTimestamp>(new DuckDBParameter(dateTime), "Timestamp");
            var instant = Split<DuckDBTimestamp>(new DuckDBParameter(withOffset), "TimestampTz");
            var date = Split<DuckDBDateOnly>(new DuckDBParameter(DateOnly.FromDateTime(dateTime)), "Date");
            var time = Split<DuckDBTimeOnly>(new DuckDBParameter(TimeOnly.FromDateTime(dateTime)), "Time");
            var timeOfTimeSpan = Split<DuckDBTimeOnly>(new DuckDBParameter(DbType.Time, dateTime.TimeOfDay), "Time");

            var same =
                (timestamp.Date.Year, timestamp.Date.Month, timestamp.Date.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (timestamp.Time.Hour, timestamp.Time.Min, timestamp.Time.Sec, timestamp.Time.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond) &&
                (instant.Date.Year, instant.Date.Month, instant.Date.Day) == (utc.Year, utc.Month, utc.Day) &&
                (instant.Time.Hour, instant.Time.Min, instant.Time.Sec, instant.Time.Microsecond) == (utc.Hour, utc.Minute, utc.Second, microsecond) &&
                (date.Year, date.Month, date.Day) == (dateTime.Year, dateTime.Month, dateTime.Day) &&
                (time.Hour, time.Min, time.Sec, time.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond) &&
                (timeOfTimeSpan.Hour, timeOfTimeSpan.Min, timeOfTimeSpan.Sec, timeOfTimeSpan.Microsecond) == (dateTime.Hour, dateTime.Minute, dateTime.Second, microsecond);

            if (!same)
            {
                Assert.Fail($"{dateTime:O} and {withOffset:O}: DuckDB has the timestamp as {timestamp.Date.Year}-{timestamp.Date.Month}-{timestamp.Date.Day} " +
                            $"{timestamp.Time.Hour}:{timestamp.Time.Min}:{timestamp.Time.Sec}.{timestamp.Time.Microsecond:D6}; " +
                            $"the instant as {instant.Date.Year}-{instant.Date.Month}-{instant.Date.Day} {instant.Time.Hour}:{instant.Time.Min}:{instant.Time.Sec}.{instant.Time.Microsecond:D6}; " +
                            $"the date as {date.Year}-{date.Month}-{date.Day}; the time as {time.Hour}:{time.Min}:{time.Sec}.{time.Microsecond:D6}; " +
                            $"the TimeSpan as {timeOfTimeSpan.Hour}:{timeOfTimeSpan.Min}:{timeOfTimeSpan.Sec}.{timeOfTimeSpan.Microsecond:D6}");
            }
        }

        T Split<T>(DuckDBParameter parameter, string type)
        {
            Command.Parameters.Clear();
            Command.Parameters.Add(parameter);

            using var reader = Command.ExecuteReader();
            reader.Read();
            reader.GetDataTypeName(0).Should().Be(type);

            return reader.GetFieldValue<T>(0);
        }
    }

    // DuckDB puts the year, month, day and time of a DateTime together. .NET takes the DateTime apart, and DuckDB
    // does the rest, which is how every value was written before the arithmetic.
    private static long DuckDBMicroseconds(DateTime value)
    {
        var date = new DuckDBDateOnly(value.Year, (byte)value.Month, (byte)value.Day);
        var time = new DuckDBTimeOnly((byte)value.Hour, (byte)value.Minute, (byte)value.Second, (int)(value.Ticks % TimeSpan.TicksPerSecond / 10));

        return NativeMethods.DateTimeHelpers.DuckDBToTimestamp(new DuckDBTimestamp(date, time)).Micros;
    }

    // The ticks of DateTime values: around both ends of the range, around 1970, where the count turns negative, with
    // each part of a microsecond, of a millisecond and of a second, and random ones.
    private static IEnumerable<long> Ticks(int randomCount)
    {
        long[] anchors = [0, DateTime.MaxValue.Ticks, DateTime.UnixEpoch.Ticks, new DateTime(1582, 10, 15).Ticks, new DateTime(1900, 3, 1).Ticks, new DateTime(2000, 2, 29).Ticks, new DateTime(2024, 12, 31).Ticks];
        long[] steps = [0, 1, 9, 10, 11, 4_999, 5_000, 5_001, 9_999, 10_000, 10_001, 4_999_999, 5_000_000, 9_999_999, 10_000_000, 10_000_001, TimeSpan.TicksPerDay - 1, TimeSpan.TicksPerDay];

        foreach (var anchor in anchors)
        {
            foreach (var step in steps)
            {
                if (anchor + step <= DateTime.MaxValue.Ticks) yield return anchor + step;
                if (anchor - step >= 0) yield return anchor - step;
            }
        }

        var random = new Random(20261012);

        for (var i = 0; i < randomCount; i++)
        {
            yield return (i % 3) switch
            {
                0 => random.NextInt64(0, DateTime.MaxValue.Ticks + 1),
                1 => random.NextInt64(new DateTime(1960, 1, 1).Ticks, new DateTime(1980, 1, 1).Ticks),
                _ => random.NextInt64(new DateTime(1900, 1, 1).Ticks, new DateTime(2100, 1, 1).Ticks)
            };
        }
    }
}
