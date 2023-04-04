using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimestamp
    {
        public DuckDBDateOnly Date { get; private set; }
        public DuckDBTimeOnly Time { get; private set; }

        public DateTime ToDateTime()
        {
            return new DateTime(
                Date.Year,
                Date.Month,
                Date.Day,
                Time.Hour,
                Time.Min,
                Time.Sec)
                .AddTicks(Time.Microsecond * 10);
        }

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