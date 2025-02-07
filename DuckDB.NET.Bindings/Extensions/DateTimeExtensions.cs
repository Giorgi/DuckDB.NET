using System;
using System.Text;

namespace DuckDB.NET.Native.Extensions;

internal static class DateTimeExtensions
{
    internal static long GetTicks(int hour, int minute, int second, int microsecond = 0)
    {
        long seconds = hour * 60 * 60 + minute * 60 + second;
        return seconds * 10_000_000 + microsecond * 10;
    }

    internal static int GetMicrosecond(this TimeSpan timeSpan)
    {
        var ticks = timeSpan.Ticks - GetTicks(timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
        return (int)(ticks / 10);
    }

#if NET6_0_OR_GREATER
    internal static int GetMicrosecond(this TimeOnly timeOnly)
    {
        var ticks = timeOnly.Ticks - GetTicks(timeOnly.Hour, timeOnly.Minute, timeOnly.Second);
        return (int)(ticks / 10);
    }
#endif
}