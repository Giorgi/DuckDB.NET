using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// One closed type per column type, so what the vector holds is a constant for the JIT in every method below.
internal sealed class NumericVectorDataReader<TStorage> : VectorDataReaderBase where TStorage : unmanaged, INumberBase<TStorage>
{
    internal unsafe NumericVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        StorageType = typeof(TStorage);
    }

    internal override byte ReadByte(ulong offset) => GetValidValue<byte>(offset);
    internal override short ReadInt16(ulong offset) => GetValidValue<short>(offset);
    internal override int ReadInt32(ulong offset) => GetValidValue<int>(offset);
    internal override long ReadInt64(ulong offset) => GetValidValue<long>(offset);
    internal override float ReadFloat(ulong offset) => GetValidValue<float>(offset);
    internal override double ReadDouble(ulong offset) => GetValidValue<double>(offset);
    internal override decimal ReadDecimal(ulong offset) => GetValidValue<decimal>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        var isFloatingNumericType = TypeExtensions.IsFloatingNumericType<T>();
        var isIntegralNumericType = TypeExtensions.IsIntegralNumericType<T>();

        if (!(isIntegralNumericType || isFloatingNumericType))
        {
            return base.GetValidValue<T>(offset);
        }

        var value = GetFieldData<TStorage>(offset);

        if (typeof(TStorage) == typeof(float) || typeof(TStorage) == typeof(double))
        {
            if (typeof(T) == typeof(TStorage))
            {
                return Unsafe.As<TStorage, T>(ref value);
            }

            if (typeof(TStorage) == typeof(float) && typeof(T) == typeof(double))
            {
                return (T)(object)(double)Unsafe.As<TStorage, float>(ref value);
            }

            //A DOUBLE is narrowed like the C# cast: digits are lost, and a value too large for float is infinity
            if (typeof(TStorage) == typeof(double) && typeof(T) == typeof(float))
            {
                return (T)(object)(float)Unsafe.As<TStorage, double>(ref value);
            }

            //A floating point value read as an integer is rounded, which the non-generic path does
            if (isIntegralNumericType)
            {
                return base.GetValidValue<T>(offset);
            }

            //What is left is decimal, which the checked conversion below handles: NaN, infinity and a value
            //out of range are refused
        }

        try
        {
            return NumericConverter.Convert<TStorage, T>(value);
        }
        catch (OverflowException)
        {
            throw CannotCast(typeof(TStorage), typeof(T));
        }
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        object value = GetFieldData<TStorage>(offset);

        // Fast path: when the boxed value already has the requested type (the common case for
        // GetValue(ordinal)/this[ordinal], which read using the column's natural ClrType), skip the
        // Convert.ChangeType machinery entirely. ChangeType does not early-out on same-type for
        // IConvertible values — it re-boxes via ToXxx() — so this also removes a redundant box.
        if (targetType == typeof(TStorage))
        {
            return value;
        }

        if (targetType.IsNumeric())
        {
            try
            {
                return Convert.ChangeType(value, targetType);
            }
            catch (OverflowException)
            {
                throw CannotCast(typeof(TStorage), targetType);
            }
        }

        throw CannotCast(typeof(TStorage), targetType);
    }
}
