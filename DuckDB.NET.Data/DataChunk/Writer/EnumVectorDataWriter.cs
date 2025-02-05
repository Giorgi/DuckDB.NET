using System;
using System.Collections.Generic;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class EnumVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private readonly DuckDBType enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);

    private readonly uint enumDictionarySize = NativeMethods.LogicalType.DuckDBEnumDictionarySize(logicalType);

    private readonly Dictionary<string, uint> enumValues = [];

    internal override bool AppendString(string value, ulong rowIndex)
    {
        if (enumValues.Count == 0)
        {
            for (uint index = 0; index < enumDictionarySize; index++)
            {
                var enumValueName = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, index).ToManagedString();
                enumValues.Add(enumValueName, index);
            }
        }

        if (enumValues.TryGetValue(value, out var enumValue))
        {
            // The following casts to byte and ushort are safe because we ensure in the constructor that the value enumDictionarySize is not too high.
            return enumType switch
            {
                DuckDBType.UnsignedTinyInt => AppendValueInternal((byte)enumValue, rowIndex),
                DuckDBType.UnsignedSmallInt => AppendValueInternal((ushort)enumValue, rowIndex),
                DuckDBType.UnsignedInteger => AppendValueInternal(enumValue, rowIndex),
                _ => throw new InvalidOperationException($"Failed to write Enum column because the internal enum type must be utinyint, usmallint, or uinteger."),
            };
        }

        throw new InvalidOperationException($"Failed to write Enum column because the value \"{value}\" is not valid.");
    }

    internal override bool AppendEnum<TEnum>(TEnum value, ulong rowIndex)
    {
        var enumValue = ConvertEnumValueToUInt64(value);
        if (enumValue < enumDictionarySize)
        {
            // The following casts to byte, ushort and uint are safe because we ensure in the constructor that the value enumDictionarySize is not too high.
            return enumType switch
            {
                DuckDBType.UnsignedTinyInt => AppendValueInternal((byte)enumValue, rowIndex),
                DuckDBType.UnsignedSmallInt => AppendValueInternal((ushort)enumValue, rowIndex),
                DuckDBType.UnsignedInteger => AppendValueInternal((uint)enumValue, rowIndex),
                _ => throw new InvalidOperationException($"Failed to write Enum column because the internal enum type must be utinyint, usmallint, or uinteger."),
            };
        }

        throw new InvalidOperationException($"Failed to write Enum column because the value is outside the range (0-{enumDictionarySize - 1}).");
    }

    private static ulong ConvertEnumValueToUInt64<TEnum>(TEnum value) where TEnum : Enum
    {
        return value.GetTypeCode() switch
        {
            TypeCode.SByte => (ulong)Convert.ToSByte(value),
            TypeCode.Byte => Convert.ToByte(value),
            TypeCode.Int16 => (ulong)Convert.ToInt16(value),
            TypeCode.UInt16 => Convert.ToUInt16(value),
            TypeCode.Int32 => (ulong)Convert.ToInt32(value),
            TypeCode.UInt32 => Convert.ToUInt32(value),
            TypeCode.Int64 => (ulong)Convert.ToInt64(value),
            TypeCode.UInt64 => Convert.ToUInt64(value),
            _ => throw new InvalidOperationException($"Failed to convert the enum value {value} to ulong."),
        };
    }

    public override void Dispose()
    {
        logicalType.Dispose();
        base.Dispose();
    }
}
