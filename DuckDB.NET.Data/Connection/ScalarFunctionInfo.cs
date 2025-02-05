using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Native;
using System;

namespace DuckDB.NET.Data.Connection;

class ScalarFunctionInfo(DuckDBLogicalType returnType, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action) : IDisposable
{
    public DuckDBLogicalType ReturnType { get; } = returnType;
    public Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> Action { get; private set; } = action;

    public void Dispose()
    {
        ReturnType.Dispose();
    }
}