using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBTimestamp
{
    public DuckDBTimestamp(DuckDBDateOnly date, DuckDBTimeOnly time)
    {
        Date = date;
        Time = time;
    }

    public DuckDBDateOnly Date { get; }
    public DuckDBTimeOnly Time { get; }

    public DateTime ToDateTime()
    {
        return new DateTime(Date.Year, Date.Month, Date.Day, Time.Hour, Time.Min, Time.Sec).AddTicks(Time.Microsecond * 10);
    }

    public static DuckDBTimestamp FromDateTime(DateTime dateTime)
    {
        return new DuckDBTimestamp(DuckDBDateOnly.FromDateTime(dateTime), DuckDBTimeOnly.FromDateTime(dateTime));
    }
}