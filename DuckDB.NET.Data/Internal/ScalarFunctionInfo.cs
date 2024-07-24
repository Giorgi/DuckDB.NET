using System;
using System.Collections.Generic;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

class ScalarFunctionInfo(IReadOnlyList<DuckDBLogicalType> parameterTypes, DuckDBLogicalType returnType, Action<VectorDataReaderBase[], VectorDataWriterBase, int> action, bool varargs) : IDisposable
{
    public bool Varargs { get; private set; } = varargs;
    public DuckDBLogicalType ReturnType { get; } = returnType;
    public IReadOnlyList<DuckDBLogicalType> ParameterTypes { get; } = parameterTypes;
    public Action<VectorDataReaderBase[], VectorDataWriterBase, int> Action { get; private set; } = action;

    public void Dispose()
    {
        foreach (var type in ParameterTypes)
        {
            type.Dispose();
        }

        ReturnType.Dispose();
    }
}