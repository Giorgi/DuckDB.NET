using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBInterval(int months, int days, ulong micros)
{
    private const ulong MillisecondsByDay = (ulong)(24 * 60 * 60 * 1e6);
    public int Months { get; } = months;

    public int Days { get; } = days;
    public ulong Micros { get; } = micros;

    public static explicit operator TimeSpan(DuckDBInterval interval)
    {
        var (timeSpan, exception) = ToTimeSpan(interval);
        return timeSpan ?? throw exception!;
    }
    public static implicit operator DuckDBInterval(TimeSpan timeSpan) => FromTimeSpan(timeSpan);

#if NET6_0_OR_GREATER
    public bool TryConvert([NotNullWhen(true)] out TimeSpan? timeSpan)
#else
    public bool TryConvert(out TimeSpan? timeSpan)
#endif
    {
        (timeSpan, var exception) = ToTimeSpan(this);
        return exception is null;
    }

    private static (TimeSpan?, Exception?) ToTimeSpan(DuckDBInterval interval)
    {
        if (interval.Months > 0)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the attribute 'Months' is greater or equal to 1"));
        }

        var days = 0;
        var micros = interval.Micros;

        if (interval.Micros >= MillisecondsByDay)
        {
            days = Convert.ToInt32(Math.Floor((double)(interval.Micros / MillisecondsByDay)));
            if (days > int.MaxValue - interval.Days)
            {
                return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the total days value is larger than {int.MaxValue}"));
            }

            if (days > 0)
            {
                micros = interval.Micros - ((ulong)days * MillisecondsByDay);
            }
            days = interval.Days + days;
        }
        else
        {
            days = interval.Days;
        }

        if (micros * 10 > long.MaxValue)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of microseconds is larger than {long.MaxValue / 10}"));
        }

        if ((ulong)days * MillisecondsByDay * 100 + micros * 10 > long.MaxValue)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of total microseconds is larger than {long.MaxValue}"));
        }

        return (new TimeSpan(days, 0, 0, 0) + new TimeSpan((long)micros * 10), null);
    }

    private static DuckDBInterval FromTimeSpan(TimeSpan timeSpan)
        => new(0, timeSpan.Days, Convert.ToUInt64(timeSpan.Ticks / 10 - new TimeSpan(timeSpan.Days, 0, 0, 0).Ticks / 10));
}