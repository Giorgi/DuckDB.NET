using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimeOnly
    {
        public byte Hour { get; set; }

        public byte Min { get; set; }

        public byte Sec { get; set; }

        public int Msec { get; set; }
        
        public DateTime ToDateTime()
        {
            var date = DuckDBDateOnly.MinValue;
            return new DateTime(date.Year, date.Month, date.Day, Hour, Min, Sec, Msec);
        }
    
        public static DuckDBTimeOnly FromDateTime(DateTime dateTime)
        {
            return new DuckDBTimeOnly {
                Hour = (byte)dateTime.Hour,
                Min = (byte)dateTime.Minute,
                Sec = (byte)dateTime.Second,
                Msec = dateTime.Millisecond
            };
        }
    
        public static explicit operator DateTime(DuckDBTimeOnly timeOnly) => timeOnly.ToDateTime();
        public static explicit operator DuckDBTimeOnly(DateTime dateTime) => FromDateTime(dateTime);
    }
}