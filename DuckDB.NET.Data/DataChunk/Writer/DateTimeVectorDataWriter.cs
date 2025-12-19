using System;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class DateTimeVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex)
    {
        if (ColumnType == DuckDBType.Date)
        {
            return AppendValueInternal(((DuckDBDateOnly)value.Date).ToDuckDBDate(), rowIndex);
        }

        var timestamp = value.ToTimestampStruct(ColumnType);

        return AppendValueInternal(timestamp, rowIndex);
    }

    internal override bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex)
    {
        if (ColumnType == DuckDBType.TimeTz)
        {
            var timeTz = value.ToTimeTzStruct();

            return AppendValueInternal(timeTz, rowIndex);
        }

        if (ColumnType == DuckDBType.TimestampTz)
        {
            var timestamp = value.ToTimestampStruct();

            return AppendValueInternal(timestamp, rowIndex);
        }

        return base.AppendDateTimeOffset(value, rowIndex);
    }

    internal override bool AppendDateOnly(DateOnly value, ulong rowIndex) => AppendValueInternal(((DuckDBDateOnly)value).ToDuckDBDate(), rowIndex);

    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);

    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
}
