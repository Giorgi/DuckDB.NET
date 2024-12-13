using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class IntervalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendTimeSpan(TimeSpan value, ulong rowIndex) => AppendValueInternal((DuckDBInterval)value, rowIndex);
}
