using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET;

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDateOnly
{
    public DuckDBDateOnly(int year, byte month, byte day)
    {
        Year = year;
        Month = month;
        Day = day;
    }

    public int Year { get; }

    public byte Month { get; }

    public byte Day { get; }

    internal static readonly DuckDBDateOnly MinValue = FromDateTime(DateTime.MinValue);

    public static DuckDBDateOnly FromDateTime(DateTime dateTime) => new DuckDBDateOnly(dateTime.Year, (byte)dateTime.Month, (byte)dateTime.Day);

    public DateTime ToDateTime() => new DateTime(Year, Month, Day);

    public static explicit operator DateTime(DuckDBDateOnly dateOnly) => dateOnly.ToDateTime();
        
    public static explicit operator DuckDBDateOnly(DateTime dateTime) => FromDateTime(dateTime);
        
#if NET6_0_OR_GREATER
        
    public static implicit operator DateOnly(DuckDBDateOnly dateOnly) => new DateOnly(dateOnly.Year, dateOnly.Month, dateOnly.Day);
        
    public static implicit operator DuckDBDateOnly(DateOnly date) => new DuckDBDateOnly(date.Year, (byte)date.Month, (byte) date.Day);
        
#endif
}