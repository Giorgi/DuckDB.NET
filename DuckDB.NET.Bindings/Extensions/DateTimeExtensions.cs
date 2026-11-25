namespace DuckDB.NET.Native.Extensions;

internal static class DateTimeExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetTicks(int hour, int minute, int second, int microsecond = 0)
    {
        long seconds = hour * 60 * 60 + minute * 60 + second;
        // In long, as DuckDB adds up the parts in int64: an int microsecond times ten wraps around beyond ±214,748,364.
        return seconds * 10_000_000 + microsecond * 10L;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetMicrosecond(this TimeSpan timeSpan)
    {
        var ticks = timeSpan.Ticks - GetTicks(timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
        return (int)(ticks / 10);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetMicrosecond(this TimeOnly timeOnly)
    {
        var ticks = timeOnly.Ticks - GetTicks(timeOnly.Hour, timeOnly.Minute, timeOnly.Second);
        return (int)(ticks / 10);
    }
}