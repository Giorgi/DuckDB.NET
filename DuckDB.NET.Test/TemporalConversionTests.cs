using System.Globalization;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// DuckDB.NET converts a DATE and the timestamp types to .NET types with its own arithmetic, without asking DuckDB.
// These tests compare its results with DuckDB's: the parts DuckDB splits the same value into (a read as
// DuckDBDateOnly or DuckDBTimestamp calls duckdb_from_date or duckdb_from_timestamp) and the text DuckDB prints
// for it. If a DuckDB release changes how these types are stored or what they mean, these tests fail.
public class TemporalConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private const long MicrosecondsAtDateTimeMinValue = -62_135_596_800_000_000;
    private const long MicrosecondsAtDateTimeMaxValue = 253_402_300_799_999_999;

    [Fact]
    public void EveryDateThatDateTimeHoldsIsReadAsDuckDBSplitsIt()
    {
        var dayCount = DateOnly.MaxValue.DayNumber + 1;

        Command.CommandText = $"SELECT DATE '0001-01-01' + i::INTEGER FROM range({dayCount}) t(i)";
        using var reader = Command.ExecuteReader();

        var dayNumber = 0;
        while (reader.Read())
        {
            var split = reader.GetFieldValue<DuckDBDateOnly>(0);
            var expected = new DateOnly(split.Year, split.Month, split.Day);

            // Compared without FluentAssertions: there are 3.6 million rows.
            if (expected.DayNumber != dayNumber ||
                reader.GetFieldValue<DateOnly>(0) != expected ||
                reader.GetDateTime(0) != expected.ToDateTime(TimeOnly.MinValue) ||
                reader.GetDateTime(0).Kind != DateTimeKind.Unspecified ||
                (DateOnly)reader.GetValue(0) != expected)
            {
                Assert.Fail($"Day {dayNumber}: DuckDB splits it into {split.Year}-{split.Month}-{split.Day}, read as DateOnly {reader.GetFieldValue<DateOnly>(0):O}, as DateTime {reader.GetDateTime(0):O}");
            }

            dayNumber++;
        }

        dayNumber.Should().Be(dayCount);
    }

    [Fact]
    public void DatesAreReadAsDuckDBPrintsThem()
    {
        Command.CommandText = "SELECT d, strftime(d, '%Y-%m-%d'), [d] FROM (SELECT DATE '0001-01-01' + (i * 997)::INTEGER d FROM range(3663) t(i))";
        using var reader = Command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            var printed = DateOnly.ParseExact(reader.GetString(1), "yyyy-MM-dd", CultureInfo.InvariantCulture);

            reader.GetFieldValue<DateOnly>(0).Should().Be(printed);
            reader.GetDateTime(0).Should().Be(printed.ToDateTime(TimeOnly.MinValue));

            // A list item is read by the second copy of the conversion, the one that returns an object.
            reader.GetFieldValue<List<DateOnly>>(2).Should().Equal(printed);
            var inList = reader.GetFieldValue<List<DateTime>>(2).Single();
            inList.Should().Be(printed.ToDateTime(TimeOnly.MinValue));
            inList.Kind.Should().Be(DateTimeKind.Unspecified);
            rows++;
        }

        rows.Should().Be(3663);
    }

    [Theory]
    [InlineData("Timestamp", "make_timestamp(v)", "epoch_us(c)", 1)]
    [InlineData("TimestampTz", "make_timestamp(v)::TIMESTAMPTZ", "epoch_us(c)", 1)]
    [InlineData("TimestampMs", "make_timestamp(v * 1000)::TIMESTAMP_MS", "epoch_us(c::TIMESTAMP)", 1_000)]
    [InlineData("TimestampS", "make_timestamp(v * 1000000)::TIMESTAMP_S", "epoch_us(c::TIMESTAMP)", 1_000_000)]
    public void TimestampsAreReadAsDuckDBSplitsThem(string type, string expression, string microseconds, long microsecondsPerUnit)
    {
        // A day inside both ends, so that a TIMESTAMPTZ stays in range whatever the time zone of the session is.
        const long microsecondsPerDay = 86_400_000_000;
        var minimum = (MicrosecondsAtDateTimeMinValue + microsecondsPerDay) / microsecondsPerUnit;
        var maximum = (MicrosecondsAtDateTimeMaxValue - microsecondsPerDay) / microsecondsPerUnit;

        var values = new List<long> { minimum, minimum + 1, -1_000_001, -1_000_000, -999_999, -1001, -1000, -999, -1, 0, 1, 999, 1000, 1001, 999_999, 1_000_000, maximum - 1, maximum };

        var random = new Random(20261012);
        for (var i = 0; i < 20_000; i++)
        {
            values.Add(random.NextInt64(minimum, maximum));
        }

        var table = "temporal_" + type.ToLowerInvariant();
        Connection.ExecuteNonQuery($"CREATE TABLE {table} (v BIGINT)");

        using (var appender = Connection.CreateAppender(table))
        {
            foreach (var value in values)
            {
                appender.CreateRow().AppendValue(value).EndRow();
            }
        }

        // The second column is the count of microseconds as DuckDB reports it. The read as DuckDBTimestamp below
        // scales by the unit this library assumes before DuckDB splits the value, so it could not show a change of unit.
        Command.CommandText = $"SELECT c, {microseconds}, [c] FROM (SELECT {expression} AS c, rowid AS r FROM {table}) ORDER BY r";
        using var reader = Command.ExecuteReader();

        reader.GetDataTypeName(0).Should().Be(type);

        var rows = 0;
        while (reader.Read())
        {
            var split = reader.GetFieldValue<DuckDBTimestamp>(0);
            var expected = new DateTime(split.Date.Year, split.Date.Month, split.Date.Day, split.Time.Hour, split.Time.Min, split.Time.Sec).AddTicks(split.Time.Microsecond * 10L);

            var inList = reader.GetFieldValue<List<DateTime>>(2)[0];

            if (reader.GetDateTime(0) != expected ||
                (DateTime)reader.GetValue(0) != expected ||
                reader.GetFieldValue<DateTimeOffset>(0) != new DateTimeOffset(expected, TimeSpan.Zero) ||
                reader.GetFieldValue<DateTimeOffset>(0).Offset != TimeSpan.Zero ||
                reader.GetDateTime(0).Kind != DateTimeKind.Unspecified ||
                ((DateTime)reader.GetValue(0)).Kind != DateTimeKind.Unspecified ||
                expected != DateTime.UnixEpoch.AddTicks(reader.GetInt64(1) * 10) ||
                inList != expected || inList.Kind != DateTimeKind.Unspecified ||
                reader.GetFieldValue<List<DateTimeOffset>>(2)[0] != new DateTimeOffset(expected, TimeSpan.Zero) ||
                reader.GetFieldValue<List<DateTimeOffset>>(2)[0].Offset != TimeSpan.Zero)
            {
                Assert.Fail($"{type}, stored {values[rows]}: DuckDB splits it into {expected:O}, read as DateTime {reader.GetDateTime(0):O}, as DateTimeOffset {reader.GetFieldValue<DateTimeOffset>(0):O}");
            }

            rows++;
        }

        rows.Should().Be(values.Count);
    }

    [Fact]
    public void TimestampsAreReadAsDuckDBPrintsThem()
    {
        Command.CommandText = "SELECT ts, strftime(ts, '%Y-%m-%d %H:%M:%S.%f'), epoch_us(ts) FROM (" +
                              $"SELECT make_timestamp({MicrosecondsAtDateTimeMinValue} + i * 86399999999977) ts FROM range(3652) t(i))";
        using var reader = Command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            var printed = DateTime.ParseExact(reader.GetString(1), "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

            reader.GetDateTime(0).Should().Be(printed);
            reader.GetDateTime(0).Should().Be(DateTime.UnixEpoch.AddTicks(reader.GetInt64(2) * 10));
            rows++;
        }

        rows.Should().Be(3652);
    }

    // A tick of a DateTime is 100 nanoseconds. The two digits below that are dropped, toward zero for both signs,
    // which is also what DuckDB does when it casts a TIMESTAMP_NS to a TIMESTAMP.
    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 0)]
    [InlineData(99, 0)]
    [InlineData(100, 1)]
    [InlineData(150, 1)]
    [InlineData(1500, 15)]
    [InlineData(-1, 0)]
    [InlineData(-99, 0)]
    [InlineData(-100, -1)]
    [InlineData(-150, -1)]
    [InlineData(-999, -9)]
    [InlineData(-1000, -10)]
    [InlineData(-1500, -15)]
    [InlineData(1_705_321_845_123_456_789, 17_053_218_451_234_567)]
    [InlineData(-1_705_321_845_123_456_789, -17_053_218_451_234_567)]
    // The whole range of a TIMESTAMP_NS is inside the range of a DateTime: 1677-09-22 to 2262-04-11 23:47:16.854775806.
    [InlineData(long.MaxValue - 1, 92_233_720_368_547_758)]
    [InlineData(-9_223_286_400_000_000_000, -92_232_864_000_000_000)]
    public void TimestampNsKeepsTheTicksAndDropsWhatIsBelowATick(long nanoseconds, long ticksFromEpoch)
    {
        Command.CommandText = $"SELECT make_timestamp_ns({nanoseconds})";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var expected = DateTime.UnixEpoch.AddTicks(ticksFromEpoch);

        reader.GetDataTypeName(0).Should().Be("TimestampNs");
        reader.GetDateTime(0).Should().Be(expected);
        reader.GetValue(0).Should().Be(expected);
        reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(expected, TimeSpan.Zero));
    }

    [Fact]
    public void TimestampNsAgreesWithDuckDBToTheMicrosecond()
    {
        Command.CommandText = "SELECT ns, epoch_us(ns::TIMESTAMP) FROM (SELECT make_timestamp_ns((i - 1844) * 5000000000000037) ns FROM range(3689) t(i))";
        using var reader = Command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            // DuckDB narrows to whole microseconds; the read keeps one more digit.
            var ticks = reader.GetDateTime(0).Ticks - DateTime.UnixEpoch.Ticks;
            (ticks / 10).Should().Be(reader.GetInt64(1));
            rows++;
        }

        rows.Should().Be(3689);
    }

    // A DuckDB timestamp reaches from 290309 BC to the year 294247 and a date further still. A value outside what
    // the .NET type holds is refused with the column in the message, and can be read as the DuckDB type.
    [Theory]
    [InlineData("make_timestamp(253402300800000000)", 10000)]
    [InlineData("make_timestamp(-62135596800000001)", 0)]
    // Two days past the end: the cast moves the value by the time zone of the session.
    [InlineData("make_timestamp(253402473600000000)::TIMESTAMPTZ", 10000)]
    [InlineData("make_timestamp(253402300800000000)::TIMESTAMP_MS", 10000)]
    [InlineData("make_timestamp(-62135596801000000)::TIMESTAMP_MS", 0)]
    [InlineData("make_timestamp(253402300800000000)::TIMESTAMP_S", 10000)]
    [InlineData("make_timestamp(-62135596801000000)::TIMESTAMP_S", 0)]
    [InlineData("TIMESTAMP '294247-01-10 04:00:54.775806'", 294247)]
    [InlineData("TIMESTAMP '290309-12-22 (BC) 00:00:00'", -290308)]
    public void TimestampOutsideTheRangeOfDateTimeIsRefused(string expression, int year)
    {
        Command.CommandText = $"SELECT {expression} AS moment";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetDateTime(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column moment is outside the range of System.DateTime*DuckDBTimestamp*");
        reader.Invoking(r => r.GetValue(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column moment is outside the range of System.DateTime*");
        reader.Invoking(r => r.GetFieldValue<DateTimeOffset>(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column moment is outside the range of System.DateTimeOffset*");

        reader.GetFieldValue<DuckDBTimestamp>(0).Date.Year.Should().Be(year);
    }

    [Theory]
    [InlineData("DATE '9999-12-31' + 1", 10000)]
    [InlineData("DATE '0001-01-01' - 1", 0)]
    [InlineData("DATE '5881580-07-10'", 5881580)]
    [InlineData("DATE '5877642-06-25 (BC)'", -5877641)]
    public void DateOutsideTheRangeOfDateTimeIsRefused(string expression, int year)
    {
        Command.CommandText = $"SELECT {expression} AS day";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetDateTime(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column day is outside the range of System.DateTime*DuckDBDateOnly*");
        reader.Invoking(r => r.GetFieldValue<DateOnly>(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column day is outside the range of System.DateOnly*");
        reader.Invoking(r => r.GetValue(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column day is outside the range of System.DateOnly*");

        reader.GetFieldValue<DuckDBDateOnly>(0).Year.Should().Be(year);
    }

    [Fact]
    public void TheLimitsOfDateTimeAreRead()
    {
        Command.CommandText = "SELECT make_timestamp(253402300799999999), make_timestamp(-62135596800000000), " +
                              "make_timestamp(253402300799999000)::TIMESTAMP_MS, make_timestamp(-62135596800000000)::TIMESTAMP_MS, " +
                              "make_timestamp(253402300799000000)::TIMESTAMP_S, make_timestamp(-62135596800000000)::TIMESTAMP_S, " +
                              "DATE '9999-12-31', DATE '0001-01-01'";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(new DateTime(DateTime.MaxValue.Ticks - 9));
        reader.GetDateTime(1).Should().Be(DateTime.MinValue);
        reader.GetDateTime(2).Should().Be(new DateTime(DateTime.MaxValue.Ticks - 9999));
        reader.GetDateTime(3).Should().Be(DateTime.MinValue);
        reader.GetDateTime(4).Should().Be(new DateTime(DateTime.MaxValue.Ticks - 9999999));
        reader.GetDateTime(5).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateOnly>(6).Should().Be(DateOnly.MaxValue);
        reader.GetFieldValue<DateOnly>(7).Should().Be(DateOnly.MinValue);
        reader.GetDateTime(6).Should().Be(DateTime.MaxValue.Date);
        reader.GetDateTime(7).Should().Be(DateTime.MinValue);
    }

    [Theory]
    [InlineData("TIMESTAMP")]
    [InlineData("TIMESTAMPTZ")]
    [InlineData("TIMESTAMP_S")]
    [InlineData("TIMESTAMP_MS")]
    [InlineData("TIMESTAMP_NS")]
    public void InfiniteTimestampIsNotAnOrdinaryValue(string type)
    {
        Command.CommandText = $"SELECT 'infinity'::{type}, '-infinity'::{type}, ['infinity'::{type}]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        for (var column = 0; column < 2; column++)
        {
            var ordinal = column;

            reader.Invoking(r => r.GetDateTime(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite timestamp value*");
            reader.Invoking(r => r.GetValue(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite timestamp value*");
            reader.Invoking(r => r.GetFieldValue<DateTimeOffset>(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite timestamp value*");
        }

        reader.Invoking(r => r.GetFieldValue<List<DateTime>>(2)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite timestamp value*");

        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();
    }

    [Fact]
    public void InfiniteDateIsNotAnOrdinaryValue()
    {
        Command.CommandText = "SELECT 'infinity'::DATE, '-infinity'::DATE, ['infinity'::DATE]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        for (var column = 0; column < 2; column++)
        {
            var ordinal = column;

            reader.Invoking(r => r.GetDateTime(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite date value*");
            reader.Invoking(r => r.GetFieldValue<DateOnly>(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite date value*");
            reader.Invoking(r => r.GetValue(ordinal)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite date value*");
        }

        reader.Invoking(r => r.GetFieldValue<List<DateOnly>>(2)).Should().Throw<InvalidOperationException>().WithMessage("Cannot convert infinite date value*");

        reader.GetFieldValue<DuckDBDateOnly>(0).IsPositiveInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBDateOnly>(1).IsNegativeInfinity.Should().BeTrue();
    }

    // DuckDB stores a timestamp below its own minimum when make_timestamp is given one, and cannot split it into
    // year, month and day. The message does not send the caller to the read as DuckDBTimestamp, which fails in DuckDB.
    [Fact]
    public void TimestampBelowTheMinimumOfDuckDBIsRefusedWithoutAdvice()
    {
        Command.CommandText = "SELECT make_timestamp(-9223372022400000001) AS moment";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetDateTime(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column moment is outside the range of System.DateTime.");
        reader.Invoking(r => r.GetValue(0)).Should().Throw<ArgumentOutOfRangeException>().WithMessage("Value in column moment is outside the range of System.DateTime.");
    }

    // The reader of an argument of a function has no column name.
    [Fact]
    public void OutOfRangeArgumentOfAFunctionIsRefusedWithoutAColumnName()
    {
        Connection.RegisterScalarFunction<DateTime, long>("temporal_ticks", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                writer.WriteValue(readers[0].GetValue<DateTime>(index).Ticks, index);
            }
        });

        Command.CommandText = "SELECT temporal_ticks(TIMESTAMP '2024-01-15 12:30:45')";
        Command.ExecuteScalar().Should().Be(new DateTime(2024, 1, 15, 12, 30, 45).Ticks);

        Command.CommandText = "SELECT temporal_ticks(make_timestamp(253402300800000000))";
        Command.Invoking(c => c.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*Value is outside the range of System.DateTime. Use GetFieldValue<DuckDBTimestamp>() to read it.*");
    }
}
