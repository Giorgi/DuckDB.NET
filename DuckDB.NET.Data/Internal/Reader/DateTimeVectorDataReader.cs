using System;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class DateTimeVectorDataReader : VectorDataReader
{
    internal unsafe DateTimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(dataPointer, validityMaskPointer, columnType)
    {
    }

    public override T GetValue<T>(ulong offset)
    {
        var (isNullable, targetType) = TypeExtensions.IsNullable<T>();

        if (DuckDBType == DuckDBType.Date)
        {
            var dateOnly = GetDateOnly(offset);

            if (targetType == typeof(DateTime))
            {
                var dateTime = (DateTime)dateOnly;
                return Unsafe.As<DateTime, T>(ref dateTime);
            }

#if NET6_0_OR_GREATER
            if (targetType == typeof(DateOnly))
            {
                var dateTime = (DateOnly)dateOnly;
                return Unsafe.As<DateOnly, T>(ref dateTime);
            }
#endif
        }
        
        if (DuckDBType == DuckDBType.Time)
        {
            var dateOnly = GetTimeOnly(offset);

            if (targetType == typeof(DateTime))
            {
                var dateTime = (DateTime)dateOnly;
                return Unsafe.As<DateTime, T>(ref dateTime);
            }

#if NET6_0_OR_GREATER
            if (targetType == typeof(TimeOnly))
            {
                var dateTime = (TimeOnly)dateOnly;
                return Unsafe.As<TimeOnly, T>(ref dateTime);
            }
#endif
        }

        if (DuckDBType == DuckDBType.Timestamp)
        {
            var dateTime = GetDateTime(offset);
            return Unsafe.As<DateTime, T>(ref dateTime);
        }

        if (DuckDBType == DuckDBType.Interval)
        {
            var interval = GetFieldData<DuckDBInterval>(offset);
            return Unsafe.As<DuckDBInterval, T>(ref interval);
        }

        return base.GetValue<T>(offset);
    }

    public override object GetValue(ulong offset, Type? targetType = null)
    {
        return DuckDBType switch
        {
            DuckDBType.Date => GetDate(offset, targetType),
            DuckDBType.Time => GetTime(offset, targetType),
            DuckDBType.Timestamp => GetDateTime(offset),
            DuckDBType.Interval => GetFieldData<DuckDBInterval>(offset),
            _ => base.GetValue(offset, targetType)
        };
    }

    private unsafe DateTime GetDateTime(ulong offset)
    {
        var data = (DuckDBTimestampStruct*)DataPointer + offset;
        return data->ToDateTime();
    }

    private DuckDBTimeOnly GetTimeOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(offset));
    }

    private DuckDBDateOnly GetDateOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(offset));
    }

    private object GetDate(ulong offset, Type? targetType = null)
    {
        var dateOnly = GetDateOnly(offset);
        if (targetType == typeof(DateTime))
        {
            return (DateTime)dateOnly;
        }

#if NET6_0_OR_GREATER
        if (targetType == typeof(DateOnly))
        {
            return (DateOnly)dateOnly;
        }
#endif

        return dateOnly;
    }

    private object GetTime(ulong offset, Type? targetType = null)
    {
        var timeOnly = GetTimeOnly(offset);
        if (targetType == typeof(DateTime))
        {
            return (DateTime)timeOnly;
        }

#if NET6_0_OR_GREATER
        if (targetType == typeof(TimeOnly))
        {
            return (TimeOnly)timeOnly;
        }
#endif

        return timeOnly;
    }
}