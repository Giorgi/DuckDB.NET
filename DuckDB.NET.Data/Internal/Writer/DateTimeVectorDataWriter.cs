using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class DateTimeVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex)
    {
        if (ColumnType == DuckDBType.Date)
        {
            return AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToDate((DuckDBDateOnly)value.Date), rowIndex);
        }

        var timestamp = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value));

        if (ColumnType == DuckDBType.TimestampNs)
        {
            timestamp.Micros *= 1000;
        }

        if (ColumnType == DuckDBType.TimestampMs)
        {
            timestamp.Micros /= 1000;
        }

        if (ColumnType == DuckDBType.TimestampS)
        {
            timestamp.Micros /= 1000000;
        }

        return AppendValueInternal(timestamp, rowIndex);
    }

    internal override bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex)
    {
        var time = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value.DateTime);
        var timeTz = NativeMethods.DateTimeHelpers.DuckDBCreateTimeTz(time.Micros, (int)value.Offset.TotalSeconds);

        return AppendValueInternal(timeTz, rowIndex);
    }

#if NET6_0_OR_GREATER
    internal override bool AppendDateOnly(DateOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToDate(value), rowIndex);

    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
#endif

    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToDate(value), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
}
