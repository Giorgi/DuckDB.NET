using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    public enum DuckDBState
    {
        DuckDBSuccess = 0,
        DuckDBError = 1
    }

    public enum DuckDBType
    {
        DuckdbTypeInvalid = 0,
        // bool
        DuckdbTypeBoolean,
        // int8_t
        DuckdbTypeTinyInt,
        // int16_t
        DuckdbTypeSmallInt,
        // int32_t
        DuckdbTypeInteger,
        // int64_t
        DuckdbTypeBigInt,
        // uint8_t
        DuckdbTypeUnsignedTinyInt,
        // uint16_t
        DuckdbTypeUnsignedSmallInt,
        // uint32_t
        DuckdbTypeUnsignedInteger,
        // uint64_t
        DuckdbTypeUnsignedBigInt,
        // float
        DuckdbTypeFloat,
        // double
        DuckdbTypeDouble,
        // duckdb_timestamp
        DuckdbTypeTimestamp,
        // duckdb_date
        DuckdbTypeDate,
        // duckdb_time
        DuckdbTypeTime,
        // duckdb_interval
        DuckdbTypeInterval,
        // duckdb_hugeint
        DuckdbTypeHugeInt,
        // const char*
        DuckdbTypeVarchar,
        // duckdb_blob
        DuckdbTypeBlob,
        DuckdbTypeDecimal,
    }

    [StructLayout(LayoutKind.Sequential)]
    public class DuckDBResult : IDisposable
    {
        [Obsolete]
        private long ColumnCount;

        [Obsolete]
        private long RowCount;

        [Obsolete]
        private long RowsChanged;

        [Obsolete]
        private IntPtr columns;

        [Obsolete]
        private IntPtr ErrorMessage;

        private IntPtr internal_data;

        public void Dispose()
        {
            NativeMethods.Query.DuckDBDestroyResult(this);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBDate
    {
        public int Days { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTime
    {
        public long Micros { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimestampStruct
    {
        public long Micros { get; set; }

        public DateTime ToDateTime()
        {
            var ticks = Micros * 10 + Utils.UnixEpochTicks;
            return new DateTime(ticks);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBBlob : IDisposable
    {
        public IntPtr Data { get; }

        public long Size { get; }

        public void Dispose()
        {
            NativeMethods.Helpers.DuckDBFree(Data);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBHugeInt
    {
        private static readonly BigInteger Base = BigInteger.Pow(2, 64);

        public static BigInteger HugeIntMinValue { get; } = BigInteger.Parse("-170141183460469231731687303715884105727");
        public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("170141183460469231731687303715884105727");

        public DuckDBHugeInt(BigInteger value)
        {
            if (value < HugeIntMinValue || value > HugeIntMaxValue)
            {
                throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
            }

            var upper = (long)BigInteger.DivRem(value, Base, out var rem);

            if (rem < 0)
            {
                rem += Base;
                upper -= 1;
            }

            Upper = upper;
            Lower = (ulong)rem;
        }

        public DuckDBHugeInt(ulong lower, long upper)
        {
            Lower = lower;
            Upper = upper;
        }

        public ulong Lower { get; }
        public long Upper { get; }

        public BigInteger ToBigInteger()
        {
            return Upper * BigInteger.Pow(2, 64) + Lower;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBDecimal
    {
        public byte Width { get; }
        public byte Scale { get; }

        public DuckDBHugeInt Value { get; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBInterval
    {
        public int Months { get; }
        public int Days { get; }
        public ulong Micros { get; }

        public DuckDBInterval(int months, int days, ulong micros)
            => (Months, Days, Micros) = (months, days, micros);

        public static explicit operator TimeSpan(DuckDBInterval interval)
        {
            (var timeSpan, var exception) = ToTimeSpan(interval);
            return timeSpan ?? throw exception;
        }
        public static implicit operator DuckDBInterval(TimeSpan timeSpan) => FromTimeSpan(timeSpan);

        public bool TryConvert(out TimeSpan? timeSpan)
        {
            (timeSpan, var exception) = ToTimeSpan(this);
            return exception is null;
        }

        private static (TimeSpan?, Exception) ToTimeSpan(DuckDBInterval interval)
        {
            if (interval.Months > 0)
                return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the attribute them is greater or equal to 1"));
            else
            {
                var milliSecondsByDay = (ulong)(24 * 60 * 60 * 1e6);
                int days = 0;
                ulong micros = interval.Micros;
                if (interval.Micros >= milliSecondsByDay)
                {
                    days = Convert.ToInt32(Math.Floor((double)(interval.Micros / milliSecondsByDay)));
                    if (days > int.MaxValue - interval.Days)
                        return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the total days value is larger than {int.MaxValue}"));

                    if (days > 0)
                        micros = interval.Micros - ((ulong)days * milliSecondsByDay);
                    days = interval.Days + days;
                }
                else
                    days = interval.Days;

                if (micros * 10 > long.MaxValue)
                    return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of microseconds is larger than {long.MaxValue / 10}"));

                if ((ulong)days * milliSecondsByDay * 100 + micros * 10 > long.MaxValue)
                    return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of total microseconds is larger than {long.MaxValue}"));

                return (new TimeSpan(days, 0, 0, 0) + new TimeSpan((long)micros * 10), null);
            }
        }



        private static DuckDBInterval FromTimeSpan(TimeSpan timeSpan)
            => new(
                    0
                    , timeSpan.Days
                    , Convert.ToUInt64(timeSpan.Ticks / 10 - new TimeSpan(timeSpan.Days, 0, 0, 0).Ticks / 10)
                );
    }
}
