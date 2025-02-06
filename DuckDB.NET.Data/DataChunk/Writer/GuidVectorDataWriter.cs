using System;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class GuidVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendGuid(Guid value, ulong rowIndex) => AppendValueInternal(value.ToHugeInt(), rowIndex);
}