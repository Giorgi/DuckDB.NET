using System;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class GuidVectorDataWriter(IntPtr vector, void* vectorData) : VectorDataWriterBase(vector, vectorData)
{
    public void AppendValue(Guid value, ulong rowIndex)
    {
        AppendValue(value.ToHugeInt(), rowIndex);
    }
}
