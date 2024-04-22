using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class DateTimeVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value)), rowIndex);

#if NET6_0_OR_GREATER
    internal override bool AppendDateOnly(DateOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToDate(value), rowIndex);

    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
#endif

    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToDate(value), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => AppendValueInternal(NativeMethods.DateTimeHelpers.DuckDBToTime(value), rowIndex);
}
