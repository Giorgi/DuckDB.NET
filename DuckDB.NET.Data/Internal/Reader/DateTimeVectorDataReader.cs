﻿using System;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class DateTimeVectorDataReader : VectorDataReaderBase
{
    private static readonly Type DateTimeType = typeof(DateTime);
    private static readonly Type DateTimeNullableType = typeof(DateTime?);

    private static readonly Type DateTimeOffsetType = typeof(DateTimeOffset);
    private static readonly Type DateTimeOffsetNullableType = typeof(DateTimeOffset?);

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

        if (DuckDBType == DuckDBType.TimeTz)
        {
            var timeTz = GetTimeTz(offset);

            if (targetType == DateTimeOffsetType || targetType == DateTimeOffsetNullableType)
            {
                var dateTimeOffset = new DateTimeOffset(timeTz.Time.ToDateTime(), TimeSpan.FromSeconds(timeTz.Offset));
                return (T)(object)dateTimeOffset;
            }

            return (T)(object)timeTz;
        }

        return DuckDBType switch
        {
            DuckDBType.Timestamp => ReadTimestamp<T>(offset, targetType),
            DuckDBType.TimestampTz => ReadTimestamp<T>(offset, targetType),
            DuckDBType.TimestampS => ReadTimestamp<T>(offset, targetType, 1000000),
            DuckDBType.TimestampMs => ReadTimestamp<T>(offset, targetType, 1000),
            DuckDBType.TimestampNs => ReadTimestamp<T>(offset, targetType, 1, 1000),
            _ => base.GetValidValue<T>(offset, targetType)
        };
    }

    private T ReadTimestamp<T>(ulong offset, Type targetType, int factor = 1, int divisor = 1)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        timestampStruct.Micros = timestampStruct.Micros * factor / divisor;

        if (targetType == DateTimeType || targetType == DateTimeNullableType)
        {
            var dateTime = timestampStruct.ToDateTime();
            return (T)(object)dateTime;
        }

        var timestamp = NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(timestampStruct);
        return (T)(object)timestamp;
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Date => GetDate(offset, targetType),
            DuckDBType.Time => GetTime(offset, targetType),
            DuckDBType.TimeTz => GetDateTimeOffset(offset, targetType),
            DuckDBType.Timestamp => GetDateTime(offset, targetType),
            DuckDBType.TimestampTz => GetDateTime(offset, targetType),
            DuckDBType.TimestampS => GetDateTime(offset, targetType, 1000000),
            DuckDBType.TimestampMs => GetDateTime(offset, targetType, 1000),
            DuckDBType.TimestampNs => GetDateTime(offset, targetType, 1, 1000),
            _ => base.GetValue(offset, targetType)
        };
    }

    private DuckDBTimeTz GetTimeTz(ulong offset)
    {
        var data = GetFieldData<DuckDBTimeTzStruct>(offset);

        return NativeMethods.DateTimeHelpers.DuckDBFromTimeTz(data);
    }

    private DuckDBTimeOnly GetTimeOnly(ulong offset)
    {
        return NativeMethods.DateTimeHelpers.DuckDBFromTime(GetFieldData<DuckDBTime>(offset));
    }

    private DuckDBDateOnly GetDateOnly(ulong offset)
    {
        return NativeMethods.DateTimeHelpers.DuckDBFromDate(GetFieldData<DuckDBDate>(offset));
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

    private object GetDateTime(ulong offset, Type targetType, int factor = 1, int divisor = 1)
    {
        var data = GetFieldData<DuckDBTimestampStruct>(offset);

        data.Micros = (data.Micros * factor / divisor);

        if (targetType == typeof(DateTime))
        {
            return data.ToDateTime();
        }

        return NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(data);
    }

    private object GetDateTimeOffset(ulong offset, Type targetType)
    {
        var timeTz = GetTimeTz(offset);

        if (targetType == typeof(DateTimeOffset))
        {
            return new DateTimeOffset(timeTz.Time.ToDateTime(), TimeSpan.FromSeconds(timeTz.Offset));
        }

        return timeTz;
    }
}