namespace DuckDB.NET.Data.DataChunk.Writer;

// A TIMETZ column: microseconds from midnight and the offset, packed into 8 bytes (DateTimeExtensions). A
// DateTimeOffset is its only type. A DateTime has no offset and used to be stored as a timestamp, whose bytes read
// back as a time and an offset that no one wrote; it is refused, as a date is.
internal sealed unsafe class TimeTzVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex) => AppendValueInternal(value.ToTimeTzStruct(), rowIndex);
}
