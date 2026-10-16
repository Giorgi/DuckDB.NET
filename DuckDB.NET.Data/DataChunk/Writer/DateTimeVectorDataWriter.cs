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

    // The DuckDB types hold a year, a month and a day, or an hour, a minute and a second. DuckDB puts them together.
    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
}
