using System;
using System.Collections;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal unsafe class VectorDataWriterBase(IntPtr vector, void* vectorData, DuckDBType columnType) : IDisposable
#if NET8_0_OR_GREATER
#pragma warning disable DuckDBNET001
    , IDuckDBDataWriter
#pragma warning restore DuckDBNET001
#endif
{
    private ulong* validity;

    internal IntPtr Vector => vector;
    internal DuckDBType ColumnType => columnType;

    public void WriteNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(Vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(Vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, rowIndex, false);
    }

    public void WriteValue<T>(T value, ulong rowIndex)
    {
        static InvalidOperationException GetIncompatibleTypeException(DuckDBType columnType, Type valueType) 
            => new($"{valueType.Name} type was passed for a {columnType} column.");
   
        if (value == null)
        {
            WriteNull(rowIndex);
            return;
        }

        if (rowIndex == 0)
        {
            var type = value.GetType();

            switch (columnType)
            {
                case DuckDBType.TinyInt: if (type != typeof(sbyte)) throw GetIncompatibleTypeException(DuckDBType.TinyInt, type); break;
                case DuckDBType.SmallInt: if (type != typeof(short)) throw GetIncompatibleTypeException(DuckDBType.SmallInt, type); break;
                case DuckDBType.Integer: if (type != typeof(int)) throw GetIncompatibleTypeException(DuckDBType.Integer, type); break;
                case DuckDBType.BigInt: if (type != typeof(long)) throw GetIncompatibleTypeException(DuckDBType.BigInt, type); break;
                case DuckDBType.UnsignedTinyInt: if (type != typeof(byte)) throw GetIncompatibleTypeException(DuckDBType.UnsignedTinyInt, type); break;
                case DuckDBType.UnsignedSmallInt: if (type != typeof(ushort)) throw GetIncompatibleTypeException(DuckDBType.UnsignedSmallInt, type); break;
                case DuckDBType.UnsignedInteger: if (type != typeof(uint)) throw GetIncompatibleTypeException(DuckDBType.UnsignedInteger, type); break;
                case DuckDBType.UnsignedBigInt: if (type != typeof(ulong)) throw GetIncompatibleTypeException(DuckDBType.UnsignedBigInt, type); break;
                case DuckDBType.Float: if (type != typeof(float)) throw GetIncompatibleTypeException(DuckDBType.Float, type); break;
                case DuckDBType.Double: if (type != typeof(double)) throw GetIncompatibleTypeException(DuckDBType.Double, type); break;
                default:
                    break;
            }
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

            Enum val => AppendEnum(val, rowIndex),

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

    internal virtual bool AppendDateTimeOffset(DateTimeOffset value, ulong rowIndex) => ThrowException<DateTimeOffset>();

    internal virtual bool AppendNumeric<T>(T value, ulong rowIndex) where T : unmanaged => ThrowException<T>();

    internal virtual bool AppendBigInteger(BigInteger value, ulong rowIndex) => ThrowException<BigInteger>();

    internal virtual bool AppendEnum<TEnum>(TEnum value, ulong rowIndex) where TEnum : Enum => ThrowException<TEnum>();

    internal virtual bool AppendCollection(ICollection value, ulong rowIndex) => ThrowException<ICollection>();

    private bool ThrowException<T>()
    {
        throw new InvalidOperationException($"Cannot write {typeof(T).Name} to {columnType} column");
    }

    internal bool AppendValueInternal<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
        return true;
    }

    internal void InitializeWriter()
    {
        validity = default;
        vectorData = NativeMethods.Vectors.DuckDBVectorGetData(Vector);
    }

    public virtual void Dispose()
    {

    }
}
