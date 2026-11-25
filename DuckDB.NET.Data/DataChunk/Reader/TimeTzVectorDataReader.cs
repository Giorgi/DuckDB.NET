namespace DuckDB.NET.Data.DataChunk.Reader;

// A TIMETZ column: microseconds from midnight and the offset, packed into 64 bits (DateTimeExtensions). As
// DateTimeOffset it is read by arithmetic; as DuckDBTimeTz DuckDB splits the value. A reader of its own, so a read has
// no test of the column type. TemporalConversionTests compares the two ways.
internal sealed class TimeTzVectorDataReader : VectorDataReaderBase
{
    // The layout of a TIMETZ is described where the constants are.
    private const int TimeTzOffsetBits = DateTimeExtensions.TimeTzOffsetBits;
    private const ulong TimeTzOffsetMask = DateTimeExtensions.TimeTzOffsetMask;
    private const int TimeTzMaximumOffsetSeconds = DateTimeExtensions.TimeTzMaximumOffsetSeconds;

    internal unsafe TimeTzVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override DateTimeOffset ReadDateTimeOffset(ulong offset) => GetValidValue<DateTimeOffset>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (typeof(T) == typeof(DateTimeOffset))
        {
            return (T)(object)GetDateTimeOffset(offset);
        }

        // DuckDBTimeTz, and any other type, which the cast refuses.
        return (T)(object)GetTimeTz(offset);
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (targetType == typeof(DateTimeOffset))
        {
            return GetDateTimeOffset(offset);
        }

        return GetTimeTz(offset);
    }

    // A DateTimeOffset needs a date, and a TIMETZ has none. The time is put on 0001-01-02, the second day a DateTime
    // holds, as Npgsql does: on the first day, a time earlier than its own positive offset would be an instant before
    // year 1, which a DateTimeOffset refuses. DuckDB allows an offset up to 15:59:59; a DateTimeOffset refuses one
    // beyond 14 hours or with seconds, and such a value is read as DuckDBTimeTz.
    private DateTimeOffset GetDateTimeOffset(ulong offset)
    {
        var bits = GetFieldData<DuckDBTimeTzStruct>(offset).Bits;

        var ticks = (long)(bits >> TimeTzOffsetBits) * TimeSpan.TicksPerMicrosecond;
        var offsetSeconds = TimeTzMaximumOffsetSeconds - (int)(bits & TimeTzOffsetMask);

        return new DateTimeOffset(new DateTime(TimeSpan.TicksPerDay + ticks), new TimeSpan(offsetSeconds * TimeSpan.TicksPerSecond));
    }

    private DuckDBTimeTz GetTimeTz(ulong offset)
    {
        var data = GetFieldData<DuckDBTimeTzStruct>(offset);

        return NativeMethods.DateTimeHelpers.DuckDBFromTimeTz(data);
    }
}
