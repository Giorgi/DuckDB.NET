namespace DuckDB.NET.Data.DataChunk.Writer;

// A TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS or TIMESTAMPTZ column: 8 bytes, a count in the unit of the column
// from 1970-01-01. A DateTime goes into any of them, a DateTimeOffset into a TIMESTAMPTZ only, as its moment in UTC.
// A date is refused (see DateVectorDataWriter), and so is a time (TimeVectorDataWriter).
internal sealed unsafe class TimestampVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex) => AppendValueInternal(value.ToTimestampStruct(ColumnType), rowIndex);

    internal override bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex) => ColumnType == DuckDBType.TimestampTz
        ? AppendValueInternal(value.ToTimestampStruct(), rowIndex)
        : base.AppendDateTimeOffset(value, rowIndex);
}
