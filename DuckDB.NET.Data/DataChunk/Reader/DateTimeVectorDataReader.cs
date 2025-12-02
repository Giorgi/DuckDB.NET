using System;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Reader;

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
            var (dateOnly, isFinite, isPositiveInfinity) = GetDateOnly(offset);

            if (!isFinite)
            {
                if (targetType == DateTimeType || targetType == DateTimeNullableType)
                {
                    var dateTime = isPositiveInfinity ? DateTime.MaxValue : DateTime.MinValue;
                    return (T)(object)dateTime;
                }

#if NET6_0_OR_GREATER
                if (targetType == DateOnlyType || targetType == DateOnlyNullableType)
                {
                    var dateOnlyValue = isPositiveInfinity ? DateOnly.MaxValue : DateOnly.MinValue;
                    return (T)(object)dateOnlyValue;
                }
#endif
                return (T)(object)dateOnly;
            }

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
            DuckDBType.Timestamp or DuckDBType.TimestampS or
            DuckDBType.TimestampTz or DuckDBType.TimestampMs or
            DuckDBType.TimestampNs => ReadTimestamp<T>(offset, targetType),
            _ => base.GetValidValue<T>(offset, targetType)
        };
    }

    private T ReadTimestamp<T>(ulong offset, Type targetType)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        if (!timestampStruct.IsFinite(DuckDBType))
        {
            if (targetType == DateTimeType || targetType == DateTimeNullableType)
            {
                var dateTime = timestampStruct.IsPositiveInfinity() ? DateTime.MaxValue : DateTime.MinValue;
                return (T)(object)dateTime;
            }

            if (targetType == DateTimeOffsetType || targetType == DateTimeOffsetNullableType)
            {
                var dateTime = timestampStruct.IsPositiveInfinity() ? DateTime.MaxValue : DateTime.MinValue;
                var dateTimeOffset = new DateTimeOffset(dateTime, TimeSpan.Zero);
                return (T)(object)dateTimeOffset;
            }

            // As of 1.4.2, duckdb_from_timestamp throws a ConversionException
            // for infinity, so infinity values are only successfully returned
            // for DateTime and DateOnly types.
            var infinityTimestamp = DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
            return (T)(object)infinityTimestamp;
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (targetType == DateTimeType || targetType == DateTimeNullableType)
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);
            return (T)(object)dateTime;
        }

        if (targetType == DateTimeOffsetType || targetType == DateTimeOffsetNullableType)
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);
            var dateTimeOffset = new DateTimeOffset(dateTime, TimeSpan.Zero);
            return (T)(object)dateTimeOffset;
        }

        return (T)(object)timestamp;
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Date => GetDate(offset, targetType),
            DuckDBType.Time => GetTime(offset, targetType),
            DuckDBType.TimeTz => GetDateTimeOffset(offset, targetType),
            DuckDBType.Timestamp or DuckDBType.TimestampS or
            DuckDBType.TimestampTz or DuckDBType.TimestampMs or
            DuckDBType.TimestampNs => GetDateTime(offset, targetType),
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

    private (DuckDBDateOnly dateOnly, bool IsFinite, bool IsPositiveInfinity) GetDateOnly(ulong offset)
    {
        var date = GetFieldData<DuckDBDate>(offset);
        var isFinite = NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(date);
        var isPositiveInfinity = date.Days == int.MaxValue;
        return (DuckDBDateOnly.FromDuckDBDate(date), isFinite, isPositiveInfinity);
    }

    private object GetDate(ulong offset, Type targetType)
    {
        var (dateOnly, isFinite, isPositiveInfinity) = GetDateOnly(offset);

        if (!isFinite)
        {
            if (targetType == DateTimeType)
            {
                return isPositiveInfinity ? DateTime.MaxValue : DateTime.MinValue;
            }

#if NET6_0_OR_GREATER
            if (targetType == DateOnlyType)
            {
                return isPositiveInfinity ? DateOnly.MaxValue : DateOnly.MinValue;
            }
#endif

            return dateOnly;
        }


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

    private object GetDateTime(ulong offset, Type targetType)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        if (!timestampStruct.IsFinite(DuckDBType))
        {
            if (targetType == typeof(DateTime))
            {
                return timestampStruct.IsPositiveInfinity() ? DateTime.MaxValue : DateTime.MinValue;
            }

            if (targetType == DateTimeOffsetType)
            {
                var dateTime = timestampStruct.IsPositiveInfinity() ? DateTime.MaxValue : DateTime.MinValue;
                return new DateTimeOffset(dateTime, TimeSpan.Zero);
            }

            // As of 1.4.2, duckdb_from_timestamp throws a ConversionException
            // for infinity, so infinity values are only successfully returned
            // for DateTime and DateOnly types.
            return timestampStruct.ToDuckDBTimestamp(DuckDBType);
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (targetType == typeof(DateTime))
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);

            return dateTime;
        }

        if (targetType == DateTimeOffsetType)
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);
            return new DateTimeOffset(dateTime, TimeSpan.Zero);
        }

        return timestamp;
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