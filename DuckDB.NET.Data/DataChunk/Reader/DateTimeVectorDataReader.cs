namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DateTimeVectorDataReader : VectorDataReaderBase
{
    internal unsafe DateTimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType == DuckDBType.Date)
        {
            var (dateOnly, isFinite) = GetDateOnly(offset);

            if (!isFinite)
            {
                if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateOnly))
                {
                    ThrowInfinityDateException();
                }

                return (T)(object)dateOnly;
            }

            if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)(DateTime)dateOnly;
            }

            if (typeof(T) == typeof(DateOnly))
            {
                return (T)(object)(DateOnly)dateOnly;
            }

            return (T)(object)dateOnly;
        }

        if (DuckDBType == DuckDBType.Time)
        {
            var timeOnly = GetTimeOnly(offset);

            if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)(DateTime)timeOnly;
            }

            if (typeof(T) == typeof(TimeOnly))
            {
                return (T)(object)(TimeOnly)timeOnly;
            }

            return (T)(object)timeOnly;
        }

        if (DuckDBType == DuckDBType.TimeTz)
        {
            var timeTz = GetTimeTz(offset);

            if (typeof(T) == typeof(DateTimeOffset))
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
            DuckDBType.TimestampNs => ReadTimestamp<T>(offset),
            _ => base.GetValidValue<T>(offset)
        };
    }

    private T ReadTimestamp<T>(ulong offset)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        if (!timestampStruct.IsFinite(DuckDBType))
        {
            if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTimeOffset))
            {
                ThrowInfinityTimestampException();
            }

            return (T)(object)DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (typeof(T) == typeof(DateTime))
        {
            return (T)(object)timestamp.ToDateTime().AddTicks(additionalTicks);
        }

        if (typeof(T) == typeof(DateTimeOffset))
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
            if (targetType == typeof(DateTime) || targetType == typeof(DateOnly))
            {
                ThrowInfinityDateException();
            }

            return dateOnly;
        }

        if (targetType == typeof(DateTime))
        {
            return (DateTime)dateOnly;
        }

        if (targetType == typeof(DateOnly))
        {
            return (DateOnly)dateOnly;
        }

        return dateOnly;
    }

    private object GetTime(ulong offset, Type targetType)
    {
        var timeOnly = GetTimeOnly(offset);
        if (targetType == typeof(DateTime))
        {
            return (DateTime)timeOnly;
        }

        if (targetType == typeof(TimeOnly))
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
            if (targetType == typeof(DateTime) || targetType == typeof(DateTimeOffset))
            {
                ThrowInfinityTimestampException();
            }

            return DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        }

        var (timestamp, additionalTicks) = timestampStruct.ToDuckDBTimestamp(DuckDBType);

        if (targetType == typeof(DateTime))
        {
            var dateTime = timestamp.ToDateTime().AddTicks(additionalTicks);

            return dateTime;
        }

        if (targetType == typeof(DateTimeOffset))
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