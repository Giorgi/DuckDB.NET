using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class BooleanVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendBool(bool value, ulong rowIndex) => AppendValueInternal(value, rowIndex);
}
