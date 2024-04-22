using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class VectorDataWriterBase(IntPtr vector, void* vectorData, DuckDBType columnType)
{
    private unsafe ulong* validity;
    internal IntPtr Vector { get; } = vector;

    public unsafe void AppendNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(Vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(Vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, rowIndex, false);
    }

    public unsafe void AppendValue<T>(T value, ulong rowIndex)
    {
        if (value == null)
        {
            AppendNull(rowIndex);
            return;
        }

        _ = value switch
        {
            string val => AppendString(val, rowIndex),
            Guid val => AppendGuid(val, rowIndex),
            TimeSpan val => AppendTimeSpan(val, rowIndex),
            _ => throw new InvalidOperationException($"Cannot write ${typeof(T).Name} to {columnType} column")
        };
    }

    internal virtual bool AppendTimeSpan(TimeSpan value, ulong rowIndex) => throw new InvalidOperationException($"Cannot write timespan to {columnType} column");

    internal virtual bool AppendGuid(Guid value, ulong rowIndex) => throw new InvalidOperationException($"Cannot write guid to {columnType} column");

    internal virtual bool AppendBlob(byte* value, int length, ulong rowIndex) => throw new InvalidOperationException($"Cannot write blob to {columnType} column");

    internal virtual bool AppendString(string value, ulong rowIndex) => throw new InvalidOperationException($"Cannot write string to {columnType} column");

    internal virtual bool AppendNumeric<T>(T value, ulong rowIndex) where T : unmanaged => throw new InvalidOperationException($"Cannot write {typeof(T).Name} to {columnType} column");

    public unsafe bool AppendValueInternal<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
        return true;
    }
}
