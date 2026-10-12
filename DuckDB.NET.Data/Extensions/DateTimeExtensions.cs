namespace DuckDB.NET.Data.Extensions;

// DuckDB stores a DATE as days and a timestamp as seconds, milliseconds, microseconds or nanoseconds, both counted
// from 1970-01-01, and a TIME as microseconds from midnight. A DateTime counts ticks of 100 nanoseconds from
// 0001-01-01 and a DateOnly counts days from there. So a .NET value is converted by arithmetic, and nothing is asked
// of DuckDB. TemporalWriteConversionTests compares the results with DuckDB's own functions.
internal static class DateTimeExtensions
{
    public const int TicksPerMicrosecond = 10;
    public const int NanosecondsPerTick = 100;

    public const int UnixEpochDayNumber = 719_162;                 // DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber
    public const long UnixEpochTicks = 621_355_968_000_000_000;    // DateTime.UnixEpoch.Ticks

    // A TIMETZ, from dtime_tz_t in DuckDB's datetime.hpp: the microseconds from midnight in the upper 40 bits, and in
    // the lower 24 the offset from UTC, stored as the largest offset (15:59:59) minus the offset, in seconds.
    public const int TimeTzOffsetBits = 24;
    public const ulong TimeTzOffsetMask = (1UL << TimeTzOffsetBits) - 1;
    public const int TimeTzMaximumOffsetSeconds = 16 * 60 * 60 - 1;

    public static DuckDBDate ToDuckDBDate(this DateOnly value) => new() { Days = value.DayNumber - UnixEpochDayNumber };

    // The time of day is dropped.
    public static DuckDBDate ToDuckDBDate(this DateTime value) => new() { Days = (int)(value.Ticks / TimeSpan.TicksPerDay) - UnixEpochDayNumber };

    // What is below a microsecond is dropped, here and in every method below but for a TIMESTAMP_NS.
    public static DuckDBTime ToDuckDBTime(this TimeOnly value) => new() { Micros = value.Ticks / TicksPerMicrosecond };

    // The date is dropped.
    public static DuckDBTime ToDuckDBTime(this DateTime value) => new() { Micros = value.Ticks % TimeSpan.TicksPerDay / TicksPerMicrosecond };

    // The time on the clock and the offset, as they are in the value: the date is dropped.
    public static DuckDBTimeTzStruct ToTimeTzStruct(this DateTimeOffset value)
    {
        var micros = value.Ticks % TimeSpan.TicksPerDay / TicksPerMicrosecond;
        var offsetSeconds = (int)(value.Offset.Ticks / TimeSpan.TicksPerSecond);

        return new DuckDBTimeTzStruct { Bits = (ulong)micros << TimeTzOffsetBits | (uint)(TimeTzMaximumOffsetSeconds - offsetSeconds) };
    }

    public static DuckDBTimestampStruct ToTimestampStruct(this DateTimeOffset value) => new() { Micros = MicrosecondsFromUnixEpoch(value.UtcTicks) };

    // The Kind of the value plays no part: the date and the time are stored as they are.
    public static DuckDBTimestampStruct ToTimestampStruct(this DateTime value, DuckDBType duckDBType)
    {
        if (duckDBType == DuckDBType.TimestampNs)
        {
            // A TIMESTAMP_NS reaches from 1677 to 2262. A value outside that range wraps around, as it always did.
            return new DuckDBTimestampStruct { Micros = (value.Ticks - UnixEpochTicks) * NanosecondsPerTick };
        }

        var micros = MicrosecondsFromUnixEpoch(value.Ticks);

        // The division rounds toward zero, so before 1970 a part of a second or of a millisecond moves the value up.
        // This division was here when DuckDB put the microseconds together, and the stored values have not changed.
        return new DuckDBTimestampStruct
        {
            Micros = duckDBType switch
            {
                DuckDBType.TimestampMs => micros / 1000,
                DuckDBType.TimestampS => micros / 1000000,
                _ => micros
            }
        };
    }

    // Each count is divided by itself: the ticks from 0001-01-01 are never negative, so what is below a microsecond
    // is dropped toward the earlier microsecond, before 1970 as after it. Dividing the difference would round a
    // value before 1970 up.
    private static long MicrosecondsFromUnixEpoch(long ticks) => ticks / TicksPerMicrosecond - UnixEpochTicks / TicksPerMicrosecond;

    public static (DuckDBTimestamp result, int additionalTicks) ToDuckDBTimestamp(this DuckDBTimestampStruct timestamp, DuckDBType duckDBType)
    {
        var additionalTicks = 0;

        if (duckDBType == DuckDBType.TimestampNs)
        {
            additionalTicks = (int)(timestamp.Micros % 1000 / 100);
            timestamp.Micros /= 1000;
        }

        if (duckDBType == DuckDBType.TimestampMs)
        {
            timestamp.Micros *= 1000;
        }

        if (duckDBType == DuckDBType.TimestampS)
        {
            timestamp.Micros *= 1000000;
        }

        var result = DuckDBTimestamp.FromDuckDBTimestampStruct(timestamp);

        return (result, additionalTicks);
    }
}
