using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class VectorDataWriterBase(IntPtr vector, void* vectorData, DuckDBType columnType)
{
    private unsafe ulong* validity;

    internal IntPtr Vector => vector;
    internal DuckDBType ColumnType => columnType;

    public unsafe void AppendNull(int rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(Vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(Vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, (ulong)rowIndex, false);
    }

    public unsafe void AppendValue<T>(T value, int rowIndex)
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
            DateTimeOffset val => AppendDateTimeOffset(val, rowIndex),
            ICollection val => AppendCollection(val, rowIndex),
            _ => ThrowException<T>()
        };
    }

    internal virtual bool AppendBool(bool value, int rowIndex) => ThrowException<bool>();

    internal virtual bool AppendDecimal(decimal value, int rowIndex) => ThrowException<decimal>();

    internal virtual bool AppendTimeSpan(TimeSpan value, int rowIndex) => ThrowException<TimeSpan>();

    internal virtual bool AppendGuid(Guid value, int rowIndex) => ThrowException<Guid>();

    internal virtual bool AppendBlob(byte* value, int length, int rowIndex) => ThrowException<byte[]>();

    internal virtual bool AppendString(string value, int rowIndex) => ThrowException<string>();

    internal virtual bool AppendDateTime(DateTime value, int rowIndex) => ThrowException<DateTime>();

#if NET6_0_OR_GREATER
    internal virtual bool AppendDateOnly(DateOnly value, int rowIndex) => ThrowException<DateOnly>();

    internal virtual bool AppendTimeOnly(TimeOnly value, int rowIndex) => ThrowException<TimeOnly>();
#endif

    internal virtual bool AppendDateOnly(DuckDBDateOnly value, int rowIndex) => ThrowException<DuckDBDateOnly>();

    internal virtual bool AppendTimeOnly(DuckDBTimeOnly value, int rowIndex) => ThrowException<DuckDBTimeOnly>();

    internal virtual bool AppendDateTimeOffset(DateTimeOffset value, int rowIndex) => ThrowException<DateTimeOffset>();

    internal virtual bool AppendNumeric<T>(T value, int rowIndex) where T : unmanaged => ThrowException<T>();

    internal virtual bool AppendBigInteger(BigInteger value, int rowIndex) => ThrowException<BigInteger>();

    internal virtual bool AppendCollection(ICollection value, int rowIndex) => ThrowException<bool>();

    private bool ThrowException<T>()
    {
        throw new InvalidOperationException($"Cannot write {typeof(T).Name} to {columnType} column");
    }

    internal unsafe bool AppendValueInternal<T>(T value, int rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
        return true;
    }

    internal void InitializerWriter()
    {
        validity = default;
        vectorData = NativeMethods.Vectors.DuckDBVectorGetData(Vector);
    }
}
