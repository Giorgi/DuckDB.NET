using DuckDB.NET.Data.Extensions;
using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class IntervalVectorDataWriter(IntPtr vector, void* vectorData) : VectorDataWriterBase(vector, vectorData)
{
    public void AppendValue(TimeSpan value, ulong rowIndex)
    {
        AppendValue((DuckDBInterval)value, rowIndex);
    }
}
