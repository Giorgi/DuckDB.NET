using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimeOnly
    {
        public DuckDBTimeOnly(byte hour, byte min, byte sec) : this(hour, min, sec, 0)
        {
        }

        public DuckDBTimeOnly(byte hour, byte min, byte sec, int msec)
        {
            Hour = hour;
            Min = min;
            Sec = sec;
            Msec = msec;
        }

        public byte Hour { get; }

        public byte Min { get; }

        public byte Sec { get; }

        public int Msec { get; }

        public DateTime ToDateTime()
        {
            var date = DuckDBDateOnly.MinValue;
            return new DateTime(date.Year, date.Month, date.Day, Hour, Min, Sec, Msec / 1000);
        }

        internal static DuckDBTimeOnly FromDateTime(DateTime dateTime)
        {
            return new DuckDBTimeOnly((byte)dateTime.Hour, (byte)dateTime.Minute, (byte)dateTime.Second, dateTime.Millisecond * 1000);
        }

        public static explicit operator DateTime(DuckDBTimeOnly timeOnly) => timeOnly.ToDateTime();
        public static explicit operator DuckDBTimeOnly(DateTime dateTime) => FromDateTime(dateTime);

#if NET6_0_OR_GREATER
        
        public static implicit operator TimeOnly(DuckDBTimeOnly time) => new(time.Hour, time.Min, time.Sec, time.Msec / 1000);

        public static implicit operator DuckDBTimeOnly(TimeOnly time) => new((byte)time.Hour, (byte)time.Minute, (byte)time.Second, time.Millisecond * 1000);
        
#endif
    }
}