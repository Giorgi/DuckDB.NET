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

            //A floating point value read as an integer is rounded: see FloatingPointToInteger
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

    // A FLOAT or DOUBLE read as an integer type: rounded to the nearest whole number, half to even, and refused when
    // that does not fit. These are the rules of Convert.ToInt32(double) and the others, which the reader has used
    // since 0.9.2. 2.0 item 5 changes this method, and only this one, to refuse a value with a fraction.
    // A float arrives as a double, exactly, as Convert.ToInt32(float) passes it on.
    private T FloatingPointToInteger<T>(double value)
    {
        try
        {
            if (typeof(T) == typeof(sbyte))  return (T)(object)Convert.ToSByte(value);
            if (typeof(T) == typeof(byte))   return (T)(object)Convert.ToByte(value);
            if (typeof(T) == typeof(short))  return (T)(object)Convert.ToInt16(value);
            if (typeof(T) == typeof(ushort)) return (T)(object)Convert.ToUInt16(value);
            if (typeof(T) == typeof(int))    return (T)(object)Convert.ToInt32(value);
            if (typeof(T) == typeof(uint))   return (T)(object)Convert.ToUInt32(value);
            if (typeof(T) == typeof(long))   return (T)(object)Convert.ToInt64(value);
            if (typeof(T) == typeof(ulong))  return (T)(object)Convert.ToUInt64(value);
        }
        catch (OverflowException)
        {
            throw CannotCast(typeof(TStorage), typeof(T));
        }

        // BigInteger
        throw CannotCast(typeof(TStorage), typeof(T));
    }

    internal override object GetValue(ulong offset, Type targetType)
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
