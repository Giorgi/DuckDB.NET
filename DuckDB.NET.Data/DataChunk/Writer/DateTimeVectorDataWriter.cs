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

    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBTime(), rowIndex);

    // A DuckDBDateOnly holds a year, a month and a day, and DuckDB puts them together. The parts of a DuckDBTimeOnly
    // are added up here; DateTimeExtensions says where that differs from DuckDB.
    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBTime(), rowIndex);
}
