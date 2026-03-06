namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DateTimeVectorDataReader : VectorDataReaderBase
{
    private static readonly Type DateTimeType = typeof(DateTime);
    private static readonly Type DateTimeOffsetType = typeof(DateTimeOffset);
    private static readonly Type DateOnlyType = typeof(DateOnly);
    private static readonly Type TimeOnlyType = typeof(TimeOnly);

    internal unsafe DateTimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Date)
        {
            var (dateOnly, isFinite) = GetDateOnly(offset);

            if (!isFinite)
            {
                if (targetType == DateTimeType || targetType == DateOnlyType)
                {
                    ThrowInfinityDateException();
                }

                return (T)(object)dateOnly;
            }

            if (targetType == DateTimeType)
            {
                return (T)(object)(DateTime)dateOnly;
            }

            if (targetType == DateOnlyType)
            {
                return (T)(object)(DateOnly)dateOnly;
            }

            return (T)(object)dateOnly;
        }

        if (DuckDBType == DuckDBType.Time)
        {
            var timeOnly = GetTimeOnly(offset);

            if (targetType == DateTimeType)
            {
                return (T)(object)(DateTime)timeOnly;
            }

            if (targetType == TimeOnlyType)
            {
                return (T)(object)(TimeOnly)timeOnly;
            }

            return (T)(object)timeOnly;
        }

        if (DuckDBType == DuckDBType.TimeTz)
        {
            var timeTz = GetTimeTz(offset);

            if (targetType == DateTimeOffsetType)
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
            if (targetType == DateTimeType || targetType == DateTimeOffsetType)
            {
                ThrowInfinityTimestampException();
            }

            return (T)(object)DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (targetType == DateTimeType)
        {
            return (T)(object)timestamp.ToDateTime().AddTicks(additionalTicks);
        }

        if (targetType == DateTimeOffsetType)
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);
            return (T)(object)new DateTimeOffset(dateTime, TimeSpan.Zero);
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

    private (DuckDBDateOnly dateOnly, bool IsFinite) GetDateOnly(ulong offset)
    {
        var date = GetFieldData<DuckDBDate>(offset);
        var isFinite = NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(date);
        return (DuckDBDateOnly.FromDuckDBDate(date), isFinite);
    }

    private object GetDate(ulong offset, Type targetType)
    {
        var (dateOnly, isFinite) = GetDateOnly(offset);

        if (!isFinite)
        {
            if (targetType == DateTimeType || targetType == DateOnlyType)
            {
                ThrowInfinityDateException();
            }

            return dateOnly;
        }

        if (targetType == DateTimeType)
        {
            return (DateTime)dateOnly;
        }

        if (targetType == DateOnlyType)
        {
            return (DateOnly)dateOnly;
        }

        return dateOnly;
    }

    private object GetTime(ulong offset, Type targetType)
    {
        var timeOnly = GetTimeOnly(offset);
        if (targetType == DateTimeType)
        {
            return (DateTime)timeOnly;
        }

        if (targetType == TimeOnlyType)
        {
            return (TimeOnly)timeOnly;
        }

        return timeOnly;
    }

    private object GetDateTime(ulong offset, Type targetType)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        if (!timestampStruct.IsFinite(DuckDBType))
        {
            if (targetType == DateTimeType || targetType == DateTimeOffsetType)
            {
                ThrowInfinityTimestampException();
            }

            return DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (targetType == DateTimeType)
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

        if (targetType == DateTimeOffsetType)
        {
            return new DateTimeOffset(timeTz.Time.ToDateTime(), TimeSpan.FromSeconds(timeTz.Offset));
        }

        return timeTz;
    }

    private static void ThrowInfinityDateException()
    {
        throw new InvalidOperationException(
            "Cannot convert infinite date value to DateTime or DateOnly. " +
            "Use DuckDBDateOnly to read this value and check IsInfinity, IsPositiveInfinity, or IsNegativeInfinity before converting to .NET types.");
    }

    private static void ThrowInfinityTimestampException()
    {
        throw new InvalidOperationException(
            "Cannot convert infinite timestamp value to DateTime or DateTimeOffset. " +
            "Use DuckDBTimestamp to read this value and check IsInfinity, IsPositiveInfinity, or IsNegativeInfinity before converting to .NET types.");
    }
}