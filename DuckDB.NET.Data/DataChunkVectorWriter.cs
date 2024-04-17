using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

unsafe class DataChunkVectorWriter(IntPtr vector, void* vectorData)
{
    private readonly IntPtr vector = vector;
    private readonly unsafe void* vectorData = vectorData;
    private unsafe ulong* validity;

    public unsafe DuckDBState AppendNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, rowIndex, false);

        return DuckDBState.Success;
    }

    public unsafe DuckDBState AppendValue<T>(T val, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = val;
        return DuckDBState.Success;
    }

    public DuckDBState AppendString(SafeUnmanagedMemoryHandle val, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElement(vector, rowIndex, val);
        return DuckDBState.Success;
    }

    public DuckDBState AppendBlob(byte* val, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(vector, rowIndex, val, length);
        return DuckDBState.Success;
    }
}