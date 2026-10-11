using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// One closed type per column type, so what the vector holds is a constant for the JIT in every method below.
internal sealed class NumericVectorDataReader<TStorage> : VectorDataReaderBase where TStorage : unmanaged, INumberBase<TStorage>
{
    internal unsafe NumericVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        StorageType = typeof(TStorage);
    }

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

            if (isFloatingNumericType)
            {
                throw CannotCast(typeof(TStorage), typeof(T));
            }

            //A floating point value read as an integer is rounded, which the non-generic path does
            return base.GetValidValue<T>(offset);
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
