namespace DuckDB.NET.Data.DataChunk.Reader;

// A TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS or TIMESTAMPTZ column: a count in the unit of the column from
// 1970-01-01. A DateTime counts ticks of 100 nanoseconds from 0001-01-01, so a read as DateTime or DateTimeOffset is a
// multiplication (a division for TIMESTAMP_NS) and an addition, and nothing is asked of DuckDB. A read as
// DuckDBTimestamp needs the year, month, day and time, and has DuckDB split the value. DATE and TIMETZ have readers of
// their own, so a read has no test of the column type. TemporalConversionTests compares the two ways.
internal sealed class TimestampVectorDataReader : VectorDataReaderBase
{
    private const long UnixEpochTicks = DateTimeExtensions.UnixEpochTicks;
    private const long MaxTicks = 3_155_378_975_999_999_999;        // DateTime.MaxValue.Ticks

    private const long MinimumDuckDBMicroseconds = -9_223_372_022_400_000_000;  // TIMESTAMP '290309-12-22 (BC) 00:00:00'

    // The ticks in one unit of the column, and the range of a DateTime in that unit. Zero for TIMESTAMP_NS, whose unit
    // is smaller than a tick and whose whole range a DateTime holds.
    private readonly long ticksPerUnit;
    private readonly long minimumUnits;
    private readonly long maximumUnits;

    internal unsafe TimestampVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
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
    internal override DateTimeOffset ReadDateTimeOffset(ulong offset) => GetValidValue<DateTimeOffset>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (typeof(T) == typeof(DateTime))
        {
            return (T)(object)new DateTime(GetTicks(offset, typeof(DateTime)));
        }

        if (typeof(T) == typeof(DateTimeOffset))
        {
            return (T)(object)new DateTimeOffset(new DateTime(GetTicks(offset, typeof(DateTimeOffset))), TimeSpan.Zero);
        }

        // DuckDBTimestamp, and any other type, which the cast refuses.
        return (T)(object)GetTimestamp(offset);
    }

    internal override object GetValueCore(ulong offset, Type targetType)
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

            // What is below a tick is dropped toward minus infinity, so that a moment before 1970 keeps its date and
            // time of day: a nanosecond before 1970 is 1969-12-31 23:59:59.9999999, not 1970-01-01. DuckDB 2.0 casts
            // a TIMESTAMP_NS to a TIMESTAMP the same way; 1.5 dropped the digits toward zero.
            var ticks = units / DateTimeExtensions.NanosecondsPerTick;

            if (units % DateTimeExtensions.NanosecondsPerTick < 0)
            {
                ticks--;
            }

            return UnixEpochTicks + ticks;
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

    // Builds the exception, so that no message is put together in the methods that read.
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
}
