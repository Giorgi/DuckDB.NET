using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBTimeOnly
{
    public DuckDBTimeOnly(byte hour, byte min, byte sec) : this(hour, min, sec, 0)
    {
    }

    public DuckDBTimeOnly(byte hour, byte min, byte sec, int microsecond)
    {
        Hour = hour;
        Min = min;
        Sec = sec;
        Microsecond = microsecond;
    }

    public byte Hour { get; }

    public byte Min { get; }

    public byte Sec { get; }

    public int Microsecond { get; }

    public long Ticks => Utils.GetTicks(Hour, Min, Sec, Microsecond);

    public DateTime ToDateTime()
    {
        var date = DuckDBDateOnly.MinValue;

        return new DateTime(date.Year, date.Month, date.Day).AddTicks(Ticks);
    }

    internal static DuckDBTimeOnly FromDateTime(DateTime dateTime)
    {
        var timeOfDay = dateTime.TimeOfDay;
        var microsecond = timeOfDay.GetMicrosecond();
        return new DuckDBTimeOnly((byte)timeOfDay.Hours, (byte)timeOfDay.Minutes, (byte)timeOfDay.Seconds, microsecond);
    }

    public static explicit operator DateTime(DuckDBTimeOnly timeOnly) => timeOnly.ToDateTime();
    public static explicit operator DuckDBTimeOnly(DateTime dateTime) => FromDateTime(dateTime);

#if NET6_0_OR_GREATER
    internal static DuckDBTimeOnly FromTimeOnly(TimeOnly timeOnly)
    {
        var microsecond = timeOnly.GetMicrosecond();
        return new DuckDBTimeOnly((byte)timeOnly.Hour, (byte)timeOnly.Minute, (byte)timeOnly.Second, microsecond);
    }

    public static implicit operator TimeOnly(DuckDBTimeOnly time) => new(time.Ticks);

    public static implicit operator DuckDBTimeOnly(TimeOnly time) => FromTimeOnly(time);

#endif
}