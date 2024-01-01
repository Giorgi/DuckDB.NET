using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.Internal.Reader;

internal class EnumVectorDataReader : VectorDataReaderBase
{
    private readonly DuckDBType enumType;
    private readonly DuckDBLogicalType logicalType;

    internal unsafe EnumVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
    }

    protected override T GetValueInternal<T>(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Enum)
        {
            return base.GetValueInternal<T>(offset, targetType);
        }

        if (!IsValid(offset))
        {
            throw new InvalidCastException($"Column '{ColumnName}' value is null");
        }

        switch (enumType)
        {
            case DuckDBType.UnsignedTinyInt:
            {
                var enumValue = GetFieldData<byte>(offset);
                return ToEnumOrString(enumValue);
            }
            case DuckDBType.UnsignedSmallInt:
            {
                var enumValue = GetFieldData<ushort>(offset);
                return ToEnumOrString(enumValue);
            }
            case DuckDBType.UnsignedInteger:
            {
                var enumValue = GetFieldData<uint>(offset);
                return ToEnumOrString(enumValue);
            }
            default:
                throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}");
        }

        T ToEnumOrString<TSource>(TSource enumValue) where TSource: unmanaged
        {
            if (typeof(T) == typeof(string))
            {
                var value = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, Convert.ToInt64(enumValue)).ToManagedString();
                return (T)(object)value;
            }
            return Unsafe.As<TSource, T>(ref enumValue);
        }
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Enum)
        {
            return GetEnum(offset, targetType);
        }

        return base.GetValue(offset, targetType);
    }

    private object GetEnum(ulong offset, Type returnType)
    {
        long enumValue = enumType switch
        {
            DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
            DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
            DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
            _ => -1
        };

        if (returnType == typeof(string))
        {
            var value = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, enumValue).ToManagedString();
            return value;
        }
        
        var enumItem = Enum.Parse(returnType, enumValue.ToString(CultureInfo.InvariantCulture));
        return enumItem;
    }

    public override void Dispose()
    {
        logicalType.Dispose();
        base.Dispose();
    }
}