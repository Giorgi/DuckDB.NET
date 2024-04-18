using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class VectorDataWriterBase(IntPtr vector, void* vectorData)
{
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

    public unsafe void AppendValue<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
    }

    public void AppendString(string value, ulong rowIndex)
    {
        using var unmanagedString = value.ToUnmanagedString();
        NativeMethods.Vectors.DuckDBVectorAssignStringElement(vector, rowIndex, unmanagedString);
    }

    public void AppendBlob(byte* value, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(vector, rowIndex, value, length);
    }
}