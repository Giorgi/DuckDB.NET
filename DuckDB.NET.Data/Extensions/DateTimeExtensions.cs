using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

//https://stackoverflow.com/a/5359304/239438
internal static class DateTimeExtensions
{
    public const int TicksPerMicrosecond = 10;
    public const int NanosecondsPerTick = 100;

    public static int Nanoseconds(this DateTime self)
    {
#if NET8_0_OR_GREATER
        return self.Nanosecond;
#else
        return (int)(self.Ticks % TimeSpan.TicksPerMillisecond % TicksPerMicrosecond) * NanosecondsPerTick;
#endif
    }

    public static DuckDBTimeTzStruct ToTimeTzStruct(this DateTimeOffset value)
    {
        var time = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value.DateTime);
        var timeTz = NativeMethods.DateTimeHelpers.DuckDBCreateTimeTz(time.Micros, (int)value.Offset.TotalSeconds);

        return timeTz;
    }

    public static DuckDBTimestampStruct ToTimestampStruct(this DateTimeOffset value)
    {
        var timestamp = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value.UtcDateTime));

        return timestamp;
    }

    public static DuckDBTimestampStruct ToTimestampStruct(this DateTime value, DuckDBType duckDBType)
    {
        var timestamp = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value));

        if (duckDBType == DuckDBType.TimestampNs)
        {
            timestamp.Micros *= 1000;

            timestamp.Micros += value.Nanoseconds();
        }

        if (duckDBType == DuckDBType.TimestampMs)
        {
            timestamp.Micros /= 1000;
        }

        if (duckDBType == DuckDBType.TimestampS)
        {
            timestamp.Micros /= 1000000;
        }

        return timestamp;
    }

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

        var result = NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(timestamp);

        return (result, additionalTicks);
    }

    public static bool IsFinite(this DuckDBTimestampStruct timestamp, DuckDBType duckDBType)
    {
        return duckDBType switch
        {
            DuckDBType.TimestampNs => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(timestamp),
            DuckDBType.TimestampMs => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(timestamp),
            DuckDBType.TimestampS => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(timestamp),
            _ => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(timestamp)
        };
    }

    public static bool IsPositiveInfinity(this DuckDBTimestampStruct timestamp)
    {
        return timestamp.Micros == long.MaxValue;
    }
}