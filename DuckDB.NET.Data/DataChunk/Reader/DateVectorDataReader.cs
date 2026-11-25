namespace DuckDB.NET.Data.DataChunk.Reader;

// A DATE column: DuckDBDate, the days from 1970-01-01. A DateOnly counts days from 0001-01-01 and a DateTime ticks
// from there, so a read as one of the .NET types is an addition and a multiplication, and nothing is asked of DuckDB.
// A read as DuckDBDateOnly needs the year, month and day, and has DuckDB split the value. TIMETZ and the timestamps
// have readers of their own, so a read has no test of the column type. TemporalConversionTests compares the two ways.
internal sealed class DateVectorDataReader : VectorDataReaderBase
{
    private const int UnixEpochDayNumber = DateTimeExtensions.UnixEpochDayNumber;
    private const int MaxDayNumber = 3_652_058;                     // DateOnly.MaxValue.DayNumber

    internal unsafe DateVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override DateTime ReadDateTime(ulong offset) => GetValidValue<DateTime>(offset);
    internal override DateOnly ReadDateOnly(ulong offset) => GetValidValue<DateOnly>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (typeof(T) == typeof(DateTime))
        {
            return (T)(object)new DateTime(GetDayNumber(offset, typeof(DateTime)) * TimeSpan.TicksPerDay);
        }

        if (typeof(T) == typeof(DateOnly))
        {
            return (T)(object)DateOnly.FromDayNumber(GetDayNumber(offset, typeof(DateOnly)));
        }

        // DuckDBDateOnly, and any other type, which the cast refuses.
        return (T)(object)DuckDBDateOnly.FromDuckDBDate(GetFieldData<DuckDBDate>(offset));
    }

    internal override object GetValueCore(ulong offset, Type targetType)
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

    // Builds the exception, so that no message is put together in the methods that read.
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
}
