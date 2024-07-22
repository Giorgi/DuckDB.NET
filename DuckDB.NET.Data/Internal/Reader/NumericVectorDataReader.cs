using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class NumericVectorDataReader : VectorDataReaderBase
{
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

    private T GetBigInteger<T>(ulong offset, bool unsigned)
    {
        var bigInteger = GetBigInteger(offset, unsigned);

        if (typeof(T) == typeof(sbyte))
        {
            return (T)(object)(sbyte)bigInteger;
        }

        if (typeof(T) == typeof(short))
        {
            return (T)(object)(short)bigInteger;
        }

        if (typeof(T) == typeof(int))
        {
            return (T)(object)(int)bigInteger;
        }

        if (typeof(T) == typeof(long))
        {
            return (T)(object)(long)bigInteger;
        }

        if (typeof(T) == typeof(uint))
        {
            return (T)(object)(uint)bigInteger;
        }

        if (typeof(T) == typeof(ulong))
        {
            return (T)(object)(ulong)bigInteger;
        }

        return (T)(object)bigInteger;
    }

    private TResult GetUnmanagedTypeValue<TQuery, TResult>(ulong offset) where TQuery : unmanaged
    {
        var value = GetFieldData<TQuery>(offset);

        if (typeof(TQuery) == typeof(TResult))
        {
            return Unsafe.As<TQuery, TResult>(ref value);
        }

        try
        {
            return (TResult)Convert.ChangeType(value, typeof(TResult));
        }
        catch (OverflowException)
        {
            throw new InvalidCastException($"Cannot cast from {value.GetType().Name} to {typeof(TResult).Name} in column {ColumnName}");
        }
    }
}