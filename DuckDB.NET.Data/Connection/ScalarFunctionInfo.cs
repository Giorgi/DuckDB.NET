using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Connection;

// What the callbacks of a registered scalar function need: the action, and the types to read its arguments as and
// write its result as. DuckDB keeps it as the function's user data and disposes it with the function.
sealed class ScalarFunctionInfo(DuckDBV2LogicalType returnType, DuckDBV2LogicalType[] parameterTypes, Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> action) : IDisposable
{
    public DuckDBV2LogicalType ReturnType { get; } = returnType;
    public DuckDBV2LogicalType[] ParameterTypes { get; } = parameterTypes;

    // Per parameter: whether it is ANY (an object parameter), whose arguments are read as the types they have.
    public bool[] IsAny { get; } = Array.ConvertAll(parameterTypes, IsAnyType);
    public Action<VectorDataReaderBase[], VectorDataWriterBase, ulong> Action { get; } = action;

    private static unsafe bool IsAnyType(DuckDBV2LogicalType type)
    {
        using var lease = type.Acquire();
        return DuckDBV2LogicalTypes.GetId(lease.Pointer) == DUCKDB_V2_LOGICAL_TYPE_ID.ANY;
    }

    public void Dispose()
    {
        ReturnType.Dispose();

        foreach (var type in ParameterTypes)
        {
            type.Dispose();
        }
    }
}
