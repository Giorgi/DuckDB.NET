using System;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class DateTimeVectorDataReader : VectorDataReaderBase
{
    private static readonly Type DateTimeType = typeof(DateTime);
    private static readonly Type DateTimeNullableType = typeof(DateTime?);
    
#if NET6_0_OR_GREATER
    private static readonly Type DateOnlyType = typeof(DateOnly);
    private static readonly Type DateOnlyNullableType = typeof(DateOnly?);

    private static readonly Type TimeOnlyType = typeof(TimeOnly);
    private static readonly Type TimeOnlyNullableType = typeof(TimeOnly?);

#endif

    internal unsafe DateTimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Date)
        {
            var dateOnly = GetDateOnly(offset);

            if (targetType == DateTimeType || targetType == DateTimeNullableType)
            {
                var dateTime = (DateTime)dateOnly;
                return (T)(object)dateTime;
            }

#if NET6_0_OR_GREATER
            if (targetType == DateOnlyType || targetType == DateOnlyNullableType)
            {
                var dateTime = (DateOnly)dateOnly;
                return (T)(object)dateTime;
            }
#endif
            return (T)(object)dateOnly;
        }

        if (DuckDBType == DuckDBType.Time)
        {
            var timeOnly = GetTimeOnly(offset);

            if (targetType == DateTimeType || targetType == DateTimeNullableType)
            {
                var dateTime = (DateTime)timeOnly;
                return (T)(object)dateTime;
            }

#if NET6_0_OR_GREATER
            if (targetType == TimeOnlyType || targetType == TimeOnlyNullableType)
            {
                var dateTime = (TimeOnly)timeOnly;
                return (T)(object)dateTime;
            }
#endif
            return (T)(object)timeOnly;
        }

        if (DuckDBType == DuckDBType.Timestamp)
        {
            var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

            if (targetType == DateTimeType || targetType == DateTimeNullableType)
            {
                var dateTime = timestampStruct.ToDateTime();
                return (T)(object)dateTime;
            }

            var timestamp = NativeMethods.DateTime.DuckDBFromTimestamp(timestampStruct);
            return (T)(object)timestamp;
        }

        return base.GetValidValue<T>(offset, targetType);
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Date => GetDate(offset, targetType),
            DuckDBType.Time => GetTime(offset, targetType),
            DuckDBType.Timestamp => GetDateTime(offset, targetType),
            _ => base.GetValue(offset, targetType)
        };
    }

    private DuckDBTimeOnly GetTimeOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromTime(GetFieldData<DuckDBTime>(offset));
    }

    private DuckDBDateOnly GetDateOnly(ulong offset)
    {
        return NativeMethods.DateTime.DuckDBFromDate(GetFieldData<DuckDBDate>(offset));
    }

    private object GetDate(ulong offset, Type targetType)
    {
        var dateOnly = GetDateOnly(offset);
        if (targetType == DateTimeType)
        {
            return (DateTime)dateOnly;
        }

#if NET6_0_OR_GREATER
        if (targetType == DateOnlyType)
        {
            return (DateOnly)dateOnly;
        }
#endif

        return dateOnly;
    }

    private object GetTime(ulong offset, Type targetType)
    {
        var timeOnly = GetTimeOnly(offset);
        if (targetType == DateTimeType)
        {
            return (DateTime)timeOnly;
        }

#if NET6_0_OR_GREATER
        if (targetType == TimeOnlyType)
        {
            return (TimeOnly)timeOnly;
        }
#endif

        return timeOnly;
    }

    private unsafe object GetDateTime(ulong offset, Type targetType)
    {
        var data = (DuckDBTimestampStruct*)DataPointer + offset;

        if (targetType == typeof(DateTime))
        {
            return data->ToDateTime();
        }
        
        return NativeMethods.DateTime.DuckDBFromTimestamp(*data);
    }
}