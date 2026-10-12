namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DateTimeVectorDataReader : VectorDataReaderBase
{
    // DuckDB stores a DATE as days and a timestamp as seconds, milliseconds, microseconds or nanoseconds, both
    // counted from 1970-01-01. A DateTime counts ticks of 100 nanoseconds from 0001-01-01 and a DateOnly counts days
    // from there. So a read as one of the .NET types is a multiplication and an addition, and nothing is asked of
    // DuckDB. A read as DuckDBDateOnly or DuckDBTimestamp needs the year, month and day, and has DuckDB split the value.
    // TemporalConversionTests compares the two ways, so a change in DuckDB shows there.
    private const int UnixEpochDayNumber = 719_162;                 // DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber
    private const int MaxDayNumber = 3_652_058;                     // DateOnly.MaxValue.DayNumber
    private const long UnixEpochTicks = 621_355_968_000_000_000;    // DateTime.UnixEpoch.Ticks
    private const long MaxTicks = 3_155_378_975_999_999_999;        // DateTime.MaxValue.Ticks
    private const long MinimumDuckDBMicroseconds = -9_223_372_022_400_000_000;  // TIMESTAMP '290309-12-22 (BC) 00:00:00'

    // For a timestamp column: the ticks in one unit of the column, and the range of a DateTime in that unit.
    // Zero for TIMESTAMP_NS, whose unit is smaller than a tick and whose whole range a DateTime holds.
    private readonly long ticksPerUnit;
    private readonly long minimumUnits;
    private readonly long maximumUnits;

    internal unsafe DateTimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        // A DATE, TIME or TIMETZ column gets the values of a microsecond column and never uses them.
        ticksPerUnit = columnType switch
        {
            DuckDBType.TimestampS => TimeSpan.TicksPerSecond,
            DuckDBType.TimestampMs => TimeSpan.TicksPerMillisecond,
            DuckDBType.TimestampNs => 0,
            _ => TimeSpan.TicksPerMicrosecond
        };

        if (ticksPerUnit > 0)
        {
            minimumUnits = -UnixEpochTicks / ticksPerUnit;
            maximumUnits = (MaxTicks - UnixEpochTicks) / ticksPerUnit;
        }
    }

    internal override DateTime ReadDateTime(ulong offset) => GetValidValue<DateTime>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType == DuckDBType.Date)
        {
            if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)new DateTime(GetDayNumber(offset, typeof(DateTime)) * TimeSpan.TicksPerDay);
            }

            if (typeof(T) == typeof(DateOnly))
            {
                return (T)(object)DateOnly.FromDayNumber(GetDayNumber(offset, typeof(DateOnly)));
            }

            return (T)(object)DuckDBDateOnly.FromDuckDBDate(GetFieldData<DuckDBDate>(offset));
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
        if (typeof(T) == typeof(DateTime))
        {
            return (T)(object)new DateTime(GetTicks(offset, typeof(DateTime)));
        }

        if (typeof(T) == typeof(DateTimeOffset))
        {
            return (T)(object)new DateTimeOffset(new DateTime(GetTicks(offset, typeof(DateTimeOffset))), TimeSpan.Zero);
        }

        return (T)(object)GetTimestamp(offset);
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

    // The days from 0001-01-01 to the date, for a date that a DateTime and a DateOnly hold.
    private int GetDayNumber(ulong offset, Type targetType)
    {
        var date = GetFieldData<DuckDBDate>(offset);
        var dayNumber = date.Days + UnixEpochDayNumber;

        // One comparison for both ends of the range. The two infinity markers are outside it, like any other
        // date that is too early or too late (a sum that wraps around is negative, and so outside as well).
        if ((uint)dayNumber > MaxDayNumber)
        {
            throw DateOutOfRange(date, targetType);
        }

        return dayNumber;
    }

    // The ticks from 0001-01-01 to the timestamp, for a timestamp that a DateTime holds.
    private long GetTicks(ulong offset, Type targetType)
    {
        // The count in the unit of the column. The field is called Micros for every unit.
        var timestamp = GetFieldData<DuckDBTimestampStruct>(offset);
        var units = timestamp.Micros;

        if (ticksPerUnit == 0)
        {
            if (timestamp.IsInfinity)
            {
                throw TimestampOutOfRange(timestamp, targetType);
            }

            // What is below a tick is dropped, toward zero for both signs. DuckDB does the same when it casts a
            // TIMESTAMP_NS to a TIMESTAMP.
            return UnixEpochTicks + units / DateTimeExtensions.NanosecondsPerTick;
        }

        // One comparison for both ends of the range, as for a date. Without it the multiplication below overflows
        // for a value far outside the range, and gives a wrong date.
        if ((ulong)(units - minimumUnits) > (ulong)(maximumUnits - minimumUnits))
        {
            throw TimestampOutOfRange(timestamp, targetType);
        }

        return UnixEpochTicks + units * ticksPerUnit;
    }

    // DuckDB splits the value into year, month, day and time. What a TIMESTAMP_NS has below a microsecond is lost.
    private DuckDBTimestamp GetTimestamp(ulong offset)
    {
        var timestampStruct = GetFieldData<DuckDBTimestampStruct>(offset);

        if (timestampStruct.IsInfinity)
        {
            return DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        }

        return timestampStruct.ToDuckDBTimestamp(DuckDBType).result;
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

    private object GetDate(ulong offset, Type targetType)
    {
        if (targetType == typeof(DateTime))
        {
            return new DateTime(GetDayNumber(offset, targetType) * TimeSpan.TicksPerDay);
        }

        if (targetType == typeof(DateOnly))
        {
            return DateOnly.FromDayNumber(GetDayNumber(offset, targetType));
        }

        return DuckDBDateOnly.FromDuckDBDate(GetFieldData<DuckDBDate>(offset));
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
        if (targetType == typeof(DateTime))
        {
            return new DateTime(GetTicks(offset, targetType));
        }

        if (targetType == typeof(DateTimeOffset))
        {
            return new DateTimeOffset(new DateTime(GetTicks(offset, targetType)), TimeSpan.Zero);
        }

        return GetTimestamp(offset);
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

    // The methods below build the exceptions, so that no message is put together in the methods that read.
    private Exception DateOutOfRange(DuckDBDate date, Type targetType)
    {
        if (date.IsInfinity)
        {
            return new InvalidOperationException(
                "Cannot convert infinite date value to DateTime or DateOnly. " +
                "Use DuckDBDateOnly to read this value and check IsInfinity, IsPositiveInfinity, or IsNegativeInfinity before converting to .NET types.");
        }

        return OutOfRange(targetType, "Use GetFieldValue<DuckDBDateOnly>() to read it.");
    }

    private Exception TimestampOutOfRange(DuckDBTimestampStruct timestamp, Type targetType)
    {
        if (timestamp.IsInfinity)
        {
            return new InvalidOperationException(
                "Cannot convert infinite timestamp value to DateTime or DateTimeOffset. " +
                "Use DuckDBTimestamp to read this value and check IsInfinity, IsPositiveInfinity, or IsNegativeInfinity before converting to .NET types.");
        }

        // DuckDB cannot split a timestamp below its own minimum either, so that one is not read as DuckDBTimestamp.
        // The scaling is for a column in seconds or milliseconds. No test reaches it: SQL does not produce such a
        // value in those units.
        var microseconds = (Int128)timestamp.Micros * ticksPerUnit / TimeSpan.TicksPerMicrosecond;

        return OutOfRange(targetType, microseconds < MinimumDuckDBMicroseconds ? "" : "Use GetFieldValue<DuckDBTimestamp>() to read it.");
    }

    private ArgumentOutOfRangeException OutOfRange(Type targetType, string advice)
    {
        // The reader of an argument of a user-defined function has no column name.
        var value = ColumnName.Length == 0 ? "Value" : $"Value in column {ColumnName}";

        return new ArgumentOutOfRangeException($"{value} is outside the range of System.{targetType.Name}. {advice}".TrimEnd(), (Exception?)null);
    }
}
