using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBDateOnly
    {
        public int Year { get; set; }

        public byte Month { get; set; }

        public byte Day { get; set; }

        internal static readonly DuckDBDateOnly MinValue = FromDateTime(DateTime.MinValue);
        
        public static DuckDBDateOnly FromDateTime(DateTime dateTime)
        {
            return new DuckDBDateOnly {
                Day = (byte)dateTime.Day,
                Month = (byte)dateTime.Month,
                Year = dateTime.Year
            };
        }
        
        public DateTime ToDateTime()
            => new DateTime(Year, Month, Day);
        
        public static explicit operator DateTime(DuckDBDateOnly dateOnly) => dateOnly.ToDateTime();
        public static explicit operator DuckDBDateOnly(DateTime dateTime) => FromDateTime(dateTime);
    }
}