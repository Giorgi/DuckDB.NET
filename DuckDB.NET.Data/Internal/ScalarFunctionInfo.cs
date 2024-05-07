using System;
using System.Collections.Generic;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

class ScalarFunctionInfo(IReadOnlyCollection<DuckDBLogicalType> parameterTypes, DuckDBLogicalType returnType, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action) : IDisposable
{
    public DuckDBLogicalType ReturnType { get; private set; } = returnType;
    public IReadOnlyCollection<DuckDBLogicalType> ParameterTypes { get; private set; } = parameterTypes;
    public Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> Action { get; private set; } = action;

    public void Dispose()
    {
        foreach (var type in ParameterTypes)
        {
            type.Dispose();
        }

        ReturnType.Dispose();
    }
}