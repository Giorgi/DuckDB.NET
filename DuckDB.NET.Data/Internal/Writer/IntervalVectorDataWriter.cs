using DuckDB.NET.Data.Extensions;
using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class IntervalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendTimeSpan(TimeSpan value, int rowIndex) => AppendValueInternal((DuckDBInterval)value, rowIndex);
}
