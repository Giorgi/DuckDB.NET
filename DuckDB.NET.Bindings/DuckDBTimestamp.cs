using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBTimestamp(DuckDBDateOnly date, DuckDBTimeOnly time)
{
    public DuckDBDateOnly Date { get; } = date;
    public DuckDBTimeOnly Time { get; } = time;

    public DateTime ToDateTime()
    {
        return new DateTime(Date.Year, Date.Month, Date.Day).AddTicks(Time.Ticks);
    }

    public static DuckDBTimestamp FromDateTime(DateTime dateTime)
    {
        return new DuckDBTimestamp(DuckDBDateOnly.FromDateTime(dateTime), DuckDBTimeOnly.FromDateTime(dateTime));
    }
}