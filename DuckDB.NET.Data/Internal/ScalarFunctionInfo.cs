using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;

namespace DuckDB.NET.Data.Internal;

class ScalarFunctionInfo(DuckDBLogicalType returnType, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action) : IDisposable
{
    public DuckDBLogicalType ReturnType { get; } = returnType;
    public Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> Action { get; private set; } = action;

    public void Dispose()
    {
        ReturnType.Dispose();
    }
}