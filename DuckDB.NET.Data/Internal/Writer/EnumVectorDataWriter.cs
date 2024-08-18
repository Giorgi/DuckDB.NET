using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class EnumVectorDataWriter : VectorDataWriterBase
{
    private readonly DuckDBLogicalType logicalType;

    private readonly DuckDBType enumType;

    private readonly uint enumDictionarySize;

    private Dictionary<string, uint>? enumValues;

    public EnumVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : base(vector, vectorData, columnType)
    {
        this.logicalType = logicalType;
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
        enumDictionarySize = NativeMethods.LogicalType.DuckDBEnumDictionarySize(logicalType);

        uint maxEnumDictionarySize = enumType switch
        {
            DuckDBType.UnsignedTinyInt => byte.MaxValue,
            DuckDBType.UnsignedSmallInt => ushort.MaxValue,
            DuckDBType.UnsignedInteger => uint.MaxValue,
            _ => throw new NotSupportedException($"The internal enum type must be utinyint, usmallint, or uinteger."),
        };
        if (enumDictionarySize > maxEnumDictionarySize)
        {
            // This exception should only be thrown if the DuckDB library has a bug.
            throw new InvalidOperationException($"The internal enum type is \"{enumType}\" but the enum dictionary size is greater than {maxEnumDictionarySize}.");
        }

        enumValues = null;
    }

    internal override bool AppendString(string value, int rowIndex)
    {
        // lazy initialization
        if (enumValues == null)
        {
            enumValues = [];
            for (uint index = 0; index < enumDictionarySize; index++)
            {
                string enumValueName = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, index).ToManagedString();
                enumValues.Add(enumValueName, index);
            }
        }

        if (enumValues.TryGetValue(value, out uint enumValue))
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

    internal override bool AppendEnum<TEnum>(TEnum value, int rowIndex)
    {
        ulong enumValue = ConvertEnumValueToUInt64(value);
        if (enumValue <= enumDictionarySize)
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

        throw new InvalidOperationException($"Failed to write Enum column because the value is outside the range (0-{enumDictionarySize}).");
    }

    private static ulong ConvertEnumValueToUInt64<TEnum>(TEnum value) where TEnum : Enum
    {
        switch (Convert.GetTypeCode(value))
        {
            case TypeCode.SByte:
                return (ulong)Convert.ToSByte(value);
            case TypeCode.Byte:
                return Convert.ToByte(value);
            case TypeCode.Int16:
                return (ulong)Convert.ToInt16(value);
            case TypeCode.UInt16: 
                return Convert.ToUInt16(value);
            case TypeCode.Int32:
                return (ulong)Convert.ToInt32(value);
            case TypeCode.UInt32: 
                return Convert.ToUInt32(value);
            case TypeCode.Int64:
                return (ulong)Convert.ToInt64(value);
            case TypeCode.UInt64:
                return Convert.ToUInt64(value);
        };

        throw new InvalidOperationException($"Failed to convert the enum value {value} to ulong.");
    }
}
