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

            //A floating point value read as an integer must be whole: see FloatingPointToInteger
            if (isIntegralNumericType)
            {
                return FloatingPointToInteger<T>(typeof(TStorage) == typeof(float) ? Unsafe.As<TStorage, float>(ref value) : Unsafe.As<TStorage, double>(ref value));
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

    // A FLOAT or DOUBLE read as an integer type: a whole value that fits reads; a value with a fraction is refused
    // (2.0 item 5; before 2.0 it was rounded half to even), and so are NaN, infinity and a value out of range, as
    // before. The same rule as a DECIMAL. A float arrives as a double, exactly. BigInteger is refused, as before.
    private T FloatingPointToInteger<T>(double value)
    {
        if (typeof(T) == typeof(BigInteger))
        {
            throw CannotCast(typeof(TStorage), typeof(T));
        }

        // NaN and infinity are not integers either, but they have no fraction to speak of: they keep the message of a
        // value out of range, as before.
        if (!double.IsInteger(value))
        {
            throw double.IsFinite(value) ? CannotCastFraction(typeof(T)) : CannotCast(typeof(TStorage), typeof(T));
        }

        try
        {
            return NumericConverter.Convert<double, T>(value);
        }
        catch (OverflowException)
        {
            throw CannotCast(typeof(TStorage), typeof(T));
        }
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        // The type the column holds, which GetValue(ordinal) and this[ordinal] ask for: the value is boxed once.
        if (targetType == typeof(TStorage))
        {
            return GetFieldData<TStorage>(offset);
        }

        return GetValueAs(offset, targetType);
    }

    // Any other type, asked for by a list, a map or a struct, which know the type only at run time. Each type is read
    // with GetValidValue, so a value converts by the same rules as it does from a column. The class is sealed, so
    // these calls are bound directly. Not inlined, so that GetValue stays as small as it was.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private object GetValueAs(ulong offset, Type targetType)
    {
        if (targetType == typeof(sbyte))      return GetValidValue<sbyte>(offset);
        if (targetType == typeof(byte))       return GetValidValue<byte>(offset);
        if (targetType == typeof(short))      return GetValidValue<short>(offset);
        if (targetType == typeof(ushort))     return GetValidValue<ushort>(offset);
        if (targetType == typeof(int))        return GetValidValue<int>(offset);
        if (targetType == typeof(uint))       return GetValidValue<uint>(offset);
        if (targetType == typeof(long))       return GetValidValue<long>(offset);
        if (targetType == typeof(ulong))      return GetValidValue<ulong>(offset);
        if (targetType == typeof(float))      return GetValidValue<float>(offset);
        if (targetType == typeof(double))     return GetValidValue<double>(offset);
        if (targetType == typeof(decimal))    return GetValidValue<decimal>(offset);
        if (targetType == typeof(BigInteger)) return GetValidValue<BigInteger>(offset);

        throw CannotCast(typeof(TStorage), targetType);
    }
}
