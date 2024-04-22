using System;
using System.Numerics;
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
            bool val => AppendBool(val, rowIndex),

            sbyte val => AppendNumeric(val, rowIndex),
            short val => AppendNumeric(val, rowIndex),
            int val => AppendNumeric(val, rowIndex),
            long val => AppendNumeric(val, rowIndex),
            byte val => AppendNumeric(val, rowIndex),
            ushort val => AppendNumeric(val, rowIndex),
            uint val => AppendNumeric(val, rowIndex),
            ulong val => AppendNumeric(val, rowIndex),
            float val => AppendNumeric(val, rowIndex),
            double val => AppendNumeric(val, rowIndex),
            
            decimal val => AppendDecimal(val, rowIndex),
            BigInteger val => AppendBigInteger(val, rowIndex),

            string val => AppendString(val, rowIndex),
            Guid val => AppendGuid(val, rowIndex),
            DateTime val => AppendDateTime(val, rowIndex),
            TimeSpan val => AppendTimeSpan(val, rowIndex),
            DuckDBDateOnly val => AppendDateOnly(val, rowIndex),
            DuckDBTimeOnly val => AppendTimeOnly(val, rowIndex),
#if NET6_0_OR_GREATER
            DateOnly val => AppendDateOnly(val, rowIndex),
            TimeOnly val => AppendTimeOnly(val, rowIndex),
#endif
            _ => ThrowException<T>()
        };
    }

    internal virtual bool AppendBool(bool value, ulong rowIndex) => ThrowException<bool>();

    internal virtual bool AppendDecimal(decimal value, ulong rowIndex) => ThrowException<decimal>();

    internal virtual bool AppendTimeSpan(TimeSpan value, ulong rowIndex) => ThrowException<TimeSpan>();

    internal virtual bool AppendGuid(Guid value, ulong rowIndex) => ThrowException<Guid>();

    internal virtual bool AppendBlob(byte* value, int length, ulong rowIndex) => ThrowException<byte[]>();

    internal virtual bool AppendString(string value, ulong rowIndex) => ThrowException<string>();

    internal virtual bool AppendDateTime(DateTime value, ulong rowIndex) => ThrowException<DateTime>();

#if NET6_0_OR_GREATER
    internal virtual bool AppendDateOnly(DateOnly value, ulong rowIndex) => ThrowException<DateOnly>();

    internal virtual bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => ThrowException<TimeOnly>();
#endif

    internal virtual bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => ThrowException<DuckDBDateOnly>();

    internal virtual bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => ThrowException<DuckDBTimeOnly>();

    internal virtual bool AppendNumeric<T>(T value, ulong rowIndex) where T : unmanaged => ThrowException<T>();

    internal virtual bool AppendBigInteger(BigInteger value, ulong rowIndex) => ThrowException<BigInteger>();

    private bool ThrowException<T>()
    {
        throw new InvalidOperationException($"Cannot write {typeof(T).Name} to {columnType} column");
    }

    internal unsafe bool AppendValueInternal<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
        return true;
    }
}
