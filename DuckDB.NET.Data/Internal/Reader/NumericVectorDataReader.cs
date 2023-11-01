using System;
using System.Numerics;

namespace DuckDB.NET.Data.Internal.Reader;

internal class NumericVectorDataReader : VectorDataReader
{
    internal unsafe NumericVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(dataPointer, validityMaskPointer, columnType)
    {
    }

    public override T GetValue<T>(ulong offset)
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
            DuckDBType.Float => GetUnmanagedTypeValue<float, T>(offset),
            DuckDBType.Double => GetUnmanagedTypeValue<double, T>(offset),
            _ => base.GetValue<T>(offset)
        };
    }

    public override object GetValue(ulong offset, Type? targetType = null)
    {
        return DuckDBType switch
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
            DuckDBType.HugeInt => GetBigInteger(offset),
            _ => base.GetValue(offset, targetType)
        };
    }

    protected unsafe BigInteger GetBigInteger(ulong offset)
    {
        var data = (DuckDBHugeInt*)DataPointer + offset;
        return data->ToBigInteger();
    }
}