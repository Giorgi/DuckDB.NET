namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class DateTimeVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex)
    {
        if (ColumnType == DuckDBType.Date)
        {
            return AppendValueInternal(value.ToDuckDBDate(), rowIndex);
        }

        return AppendValueInternal(value.ToTimestampStruct(ColumnType), rowIndex);
    }

    internal override bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex)
    {
        if (ColumnType == DuckDBType.TimeTz)
        {
            return AppendValueInternal(value.ToTimeTzStruct(), rowIndex);
        }

        if (ColumnType == DuckDBType.TimestampTz)
        {
            return AppendValueInternal(value.ToTimestampStruct(), rowIndex);
        }

        return base.AppendDateTimeOffset(value, rowIndex);
    }

    internal override bool AppendDateOnly(DateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    // A DuckDBDateOnly holds a year, a month and a day, and DuckDB puts them together.
    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    // TIME and TIME_NS have a writer of their own, TimeVectorDataWriter. So a TimeOnly or a DuckDBTimeOnly is refused
    // here: it used to be stored as a TIME, which in a DATE column wrote 8-byte values into a 4-byte vector, past its
    // end from row 1024 on.
}
