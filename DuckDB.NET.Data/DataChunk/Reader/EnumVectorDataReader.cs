using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class EnumVectorDataReader : VectorDataReaderBase
{
    private readonly DuckDBType enumType;
    private readonly DuckDBLogicalType logicalType;
    private readonly Dictionary<long, string> cachedNames = new(8);

    internal unsafe EnumVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
    }

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Enum)
        {
            return base.GetValidValue<T>(offset);
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

        T ToEnumOrString<TSource>(TSource enumValue) where TSource: IBinaryNumber<TSource>
        {
            if (typeof(T) == typeof(string))
            {
                return (T)(object)GetEnumName(long.CreateChecked(enumValue));
            }

            if (typeof(T).IsEnum)
            {
                return (T)ConvertToTargetEnum(long.CreateChecked(enumValue), typeof(T));
            }

            return Unsafe.As<TSource, T>(ref enumValue);
        }
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Enum)
        {
            long enumValue = enumType switch
            {
                DuckDBType.UnsignedTinyInt => GetFieldData<byte>(offset),
                DuckDBType.UnsignedSmallInt => GetFieldData<ushort>(offset),
                DuckDBType.UnsignedInteger => GetFieldData<uint>(offset),
                _ => throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}")
            };

            if (targetType == typeof(string))
            {
                return GetEnumName(enumValue);
            }

            if (targetType.IsEnum)
            {
                return ConvertToTargetEnum(enumValue, targetType);
            }

            return Enum.ToObject(targetType, enumValue);
        }

        return base.GetValue(offset, targetType);
    }

    public override void Dispose()
    {
        logicalType.Dispose();
        base.Dispose();
    }

    private string GetEnumName(long enumValue)
    {
        if (!cachedNames.TryGetValue(enumValue, out var name))
        {
            cachedNames[enumValue] = name = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, enumValue);
        }

        return name;
    }

    private object ConvertToTargetEnum(long enumValue, Type targetType)
    {
        var enumName = GetEnumName(enumValue);
        if (Enum.TryParse(targetType, enumName, true, out var parsedEnum))
        {
            return parsedEnum;
        }

        throw new InvalidCastException($"Cannot convert DuckDB enum value \"{enumName}\" to {targetType.Name}.");
    }
}