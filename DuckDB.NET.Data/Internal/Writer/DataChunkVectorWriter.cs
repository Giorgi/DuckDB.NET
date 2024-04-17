using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

unsafe class DataChunkVectorWriter(IntPtr vector, void* vectorData)
{
    private readonly IntPtr vector = vector;
    private readonly unsafe void* vectorData = vectorData;
    private unsafe ulong* validity;

    public unsafe void AppendNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, rowIndex, false);
    }

    public unsafe void AppendValue<T>(T val, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = val;
    }

    public void AppendString(SafeUnmanagedMemoryHandle val, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElement(vector, rowIndex, val);
    }

    public void AppendBlob(byte* val, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(vector, rowIndex, val, length);
    }
}