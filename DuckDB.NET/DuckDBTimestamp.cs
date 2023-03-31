using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimestamp
    {
        public DuckDBDateOnly Date { get; private set; }
        public DuckDBTimeOnly Time { get; private set; }

        public DateTime ToDateTime() =>
            new DateTime(
                Date.Year,
                Date.Month,
                Date.Day,
                Time.Hour,
                Time.Min,
                Time.Sec,
                Time.Msec / 1000
            );

        public static DuckDBTimestamp FromDateTime(DateTime dateTime)
        {
            return new DuckDBTimestamp
            {
                Date = DuckDBDateOnly.FromDateTime(dateTime),
                Time = DuckDBTimeOnly.FromDateTime(dateTime)
            };
        }
    }
}