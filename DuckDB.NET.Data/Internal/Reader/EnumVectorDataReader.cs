using System;
using System.Globalization;

namespace DuckDB.NET.Data.Internal.Reader;

internal class EnumVectorDataReader : VectorDataReader
{
    private readonly DuckDBType enumType;
    private readonly DuckDBLogicalType logicalType;

    internal unsafe EnumVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(dataPointer, validityMaskPointer, columnType)
    {
        logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector); 
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
    }

    public override object GetValue(ulong offset, Type? targetType = null)
    {
        return GetEnum(offset, targetType ?? ClrType);
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

        var underlyingType = Nullable.GetUnderlyingType(returnType);
        if (underlyingType != null)
        {
            if (!IsValid(offset))
            {
                return default!;
            }
            returnType = underlyingType;
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