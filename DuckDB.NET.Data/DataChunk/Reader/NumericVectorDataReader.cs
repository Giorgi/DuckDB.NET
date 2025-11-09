using System;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class NumericVectorDataReader : VectorDataReaderBase
{
    private const int VarIntHeaderSize = 3;

    internal unsafe NumericVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        var isFloatingNumericType = TypeExtensions.IsFloatingNumericType<T>();
        var isIntegralNumericType = TypeExtensions.IsIntegralNumericType<T>();

        if (!(isIntegralNumericType || isFloatingNumericType))
        {
            return base.GetValidValue<T>(offset, targetType);
        }

        //If T is integral type and column is also integral read the data and use Unsafe.As<> or Convert.ChangeType to change type
        //If T is floating and column is floating too, read data and cast to T
        //Otherwise use the non-generic path
        if (isIntegralNumericType)
        {
            return DuckDBType switch
            {
                DuckDBType.TinyInt => GetUnmanagedTypeValue<sbyte, T>(offset),
                DuckDBType.SmallInt => GetUnmanagedTypeValue<short, T>(offset),
                DuckDBType.Integer => GetUnmanagedTypeValue<int, T>(offset),
                DuckDBType.BigInt => GetUnmanagedTypeValue<long, T>(offset),
                DuckDBType.UnsignedTinyInt => GetUnmanagedTypeValue<byte, T>(offset),
                DuckDBType.UnsignedSmallInt => GetUnmanagedTypeValue<ushort, T>(offset),
                DuckDBType.UnsignedInteger => GetUnmanagedTypeValue<uint, T>(offset),
                DuckDBType.UnsignedBigInt => GetUnmanagedTypeValue<ulong, T>(offset),
                DuckDBType.HugeInt => GetBigInteger<T>(offset, false),
                DuckDBType.UnsignedHugeInt => GetBigInteger<T>(offset, true),
                DuckDBType.VarInt => GetBigInteger<T>(offset),
                _ => base.GetValidValue<T>(offset, targetType)
            };
        }

        return DuckDBType switch
        {
            DuckDBType.Float => (T)(object)GetFieldData<float>(offset),
            DuckDBType.Double => (T)(object)GetFieldData<double>(offset),
            _ => base.GetValidValue<T>(offset, targetType)
        };
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        var value = DuckDBType switch
        {
            DuckDBType.TinyInt => GetFieldData<sbyte>(offset),
            DuckDBType.SmallInt => GetFieldData<short>(offset),
            DuckDBType.Integer => GetFieldData<int>(offset),
            DuckDBType.BigInt => GetFieldData<long>(offset),
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
            DuckDBType.UnsignedBigInt => GetFieldData<ulong>(offset),
            DuckDBType.Float => GetFieldData<float>(offset),
            DuckDBType.Double => GetFieldData<double>(offset),
            DuckDBType.HugeInt => GetBigInteger(offset, false),
            DuckDBType.UnsignedHugeInt => GetBigInteger(offset, true),
            DuckDBType.VarInt => GetBigInteger<BigInteger>(offset),
            _ => base.GetValue(offset, targetType)
        };

        if (targetType.IsNumeric())
        {
            try
            {
                return Convert.ChangeType(value, targetType);
            }
            catch (OverflowException)
            {
                throw new InvalidCastException($"Cannot cast from {value.GetType().Name} to {targetType.Name} in column {ColumnName}");
            }
        }

        throw new InvalidCastException($"Cannot cast from {value.GetType().Name} to {targetType.Name} in column {ColumnName}");
    }

    internal unsafe BigInteger GetBigInteger(ulong offset, bool unsigned)
    {
        if (unsigned)
        {
            var unsignedHugeInt = ((DuckDBUHugeInt*)DataPointer + offset);
            return unsignedHugeInt->ToBigInteger();
        }
        else
        {
            var hugeInt = (DuckDBHugeInt*)DataPointer + offset;
            return hugeInt->ToBigInteger();
        }
    }

    private unsafe T GetBigInteger<T>(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        if (data->Length < VarIntHeaderSize + 1)
        {
            throw new DuckDBException("Invalid blob size for Varint.");
        }

        var buffer = new ReadOnlySpan<byte>(data->Data, data->Length);

        var isNegative = (buffer[0] & 0x80) == 0;

        // Allocate byte array once with proper capacity
        var byteCount = data->Length - VarIntHeaderSize;
        var bytes = new byte[byteCount];
        var currentByteCount = byteCount;

        for (var index = 0; index < byteCount; index++)
        {
            if (isNegative)
            {
                bytes[index] = (byte)~buffer[index + VarIntHeaderSize];
            }
            else
            {
                bytes[index] = buffer[index + VarIntHeaderSize];
            }
        }

        // Estimate maximum digit count: log10(256^n) = n * log10(256) ≈ n * 2.408
        var maxDigits = (int)(byteCount * 2.5) + 2;
        var digitChars = new char[maxDigits];
        var digitCount = 0;

        // Preallocate quotient buffer
        var quotient = new byte[byteCount];

        while (currentByteCount > 0)
        {
            byte remainder = 0;
            var quotientCount = 0;

            for (var i = 0; i < currentByteCount; i++)
            {
                var newValue = remainder * 256 + bytes[i];
                var digit = (byte)(newValue / 10);
                
                // Only add non-zero or if we already have digits
                if (digit != 0 || quotientCount > 0)
                {
                    quotient[quotientCount++] = digit;
                }

                remainder = (byte)(newValue % 10);
            }

            digitChars[digitCount++] = DigitToChar(remainder);

            // Swap buffers to avoid allocation
            var temp = bytes;
            bytes = quotient;
            quotient = temp;
            currentByteCount = quotientCount;
        }

        if (isNegative)
        {
            digitChars[digitCount++] = '-';
        }

        // Reverse the digits in place
        Array.Reverse(digitChars, 0, digitCount);

#if NET6_0_OR_GREATER
        var integer = BigInteger.Parse(digitChars.AsSpan(0, digitCount));
#else
        var integer = BigInteger.Parse(new string(digitChars, 0, digitCount));
#endif
        
        try
        {
            return CastTo<T>(integer);
        }
        catch (OverflowException)
        {
            throw new InvalidCastException($"Cannot cast from {nameof(BigInteger)} to {typeof(T).Name} in column {ColumnName}");
        }

        char DigitToChar(int c) => (char)(c + '0');
    }

    private T GetBigInteger<T>(ulong offset, bool unsigned)
    {
        var bigInteger = GetBigInteger(offset, unsigned);

        try
        {
            return CastTo<T>(bigInteger);
        }
        catch (OverflowException)
        {
            throw new InvalidCastException($"Cannot cast from {nameof(BigInteger)} to {typeof(T).Name} in column {ColumnName}");
        }
    }

    private static T CastTo<T>(BigInteger bigInteger)
    {
        if (typeof(T) == typeof(byte))
        {
            return (T)(object)(byte)bigInteger;
        }

        if (typeof(T) == typeof(sbyte))
        {
            return (T)(object)(sbyte)bigInteger;
        }

        if (typeof(T) == typeof(short))
        {
            return (T)(object)(short)bigInteger;
        }

        if (typeof(T) == typeof(ushort))
        {
            return (T)(object)(ushort)bigInteger;
        }

        if (typeof(T) == typeof(int))
        {
            return (T)(object)(int)bigInteger;
        }

        if (typeof(T) == typeof(uint))
        {
            return (T)(object)(uint)bigInteger;
        }

        if (typeof(T) == typeof(long))
        {
            return (T)(object)(long)bigInteger;
        }

        if (typeof(T) == typeof(ulong))
        {
            return (T)(object)(ulong)bigInteger;
        }

        return (T)(object)bigInteger;
    }

    private TResult GetUnmanagedTypeValue<TQuery, TResult>(ulong offset) where TQuery : unmanaged
#if NET8_0_OR_GREATER
        , INumberBase<TQuery> 
#endif
    {
        var resultType = typeof(TResult);
        var value = GetFieldData<TQuery>(offset);

        if (typeof(TQuery) == resultType)
        {
            return Unsafe.As<TQuery, TResult>(ref value);
        }

        try
        {
#if NET8_0_OR_GREATER
            if (resultType == typeof(byte))
            {
                return (TResult)(object)byte.CreateChecked(value);
            }
            if (resultType == typeof(sbyte))
            {
                return (TResult)(object)sbyte.CreateChecked(value);
            }
            if (resultType == typeof(short))
            {
                return (TResult)(object)short.CreateChecked(value);
            }
            if (resultType == typeof(ushort))
            {
                return (TResult)(object)ushort.CreateChecked(value);
            }
            if (resultType == typeof(int))
            {
                return (TResult)(object)int.CreateChecked(value);
            }
            if (resultType == typeof(uint))
            {
                return (TResult)(object)uint.CreateChecked(value);
            }
            if (resultType == typeof(long))
            {
                return (TResult)(object)long.CreateChecked(value);
            }
            if (resultType == typeof(ulong))
            {
                return (TResult)(object)ulong.CreateChecked(value);
            }
#endif

            return (TResult)Convert.ChangeType(value, resultType);
        }
        catch (OverflowException)
        {
            throw new InvalidCastException($"Cannot cast from {value.GetType().Name} to {resultType.Name} in column {ColumnName}");
        }
    }
}