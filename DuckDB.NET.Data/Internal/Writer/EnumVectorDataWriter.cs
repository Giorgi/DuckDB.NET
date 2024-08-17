using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class EnumVectorDataWriter : VectorDataWriterBase
{
    private readonly DuckDBType enumType;

    private readonly Dictionary<string, long> enumValueIndexDictionary;

    public EnumVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : base(vector, vectorData, columnType)
    {
        enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);
        uint size = NativeMethods.LogicalType.DuckDBEnumDictionarySize(logicalType);
        enumValueIndexDictionary = [];
        for (long index = 0; index < size; index++)
        {
            string enumValue = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, index).ToManagedString();
            enumValueIndexDictionary.Add(enumValue, index);
        }
    }

    internal override bool AppendString(string value, int rowIndex)
    {
        switch (enumType)
        {
            case DuckDBType.UnsignedTinyInt:
                {
                    if (enumValueIndexDictionary.TryGetValue(value, out long enumValueIndex) && 
                        enumValueIndex >= byte.MinValue && enumValueIndex <= byte.MaxValue)
                    {
                        return AppendValueInternal((byte)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            case DuckDBType.UnsignedSmallInt:
                {
                    if (enumValueIndexDictionary.TryGetValue(value, out long enumValueIndex) && 
                        enumValueIndex >= ushort.MinValue && enumValueIndex <= ushort.MaxValue)
                    {
                        return AppendValueInternal((ushort)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            case DuckDBType.UnsignedInteger:
                {
                    if (enumValueIndexDictionary.TryGetValue(value, out long enumValueIndex) &&
                        enumValueIndex >= uint.MinValue && enumValueIndex <= uint.MaxValue)
                    {
                        return AppendValueInternal((uint)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            default:
                return false;
        }
    }

    internal override bool AppendEnum<TEnum>(TEnum value, int rowIndex)
    {
        switch (enumType)
        {
            case DuckDBType.UnsignedTinyInt:
                {
                    long enumValueIndex = Convert.ToInt64(value);
                    if (enumValueIndex >= byte.MinValue && enumValueIndex <= byte.MaxValue)
                    {
                        return AppendValueInternal((byte)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            case DuckDBType.UnsignedSmallInt:
                {
                    long enumValueIndex = Convert.ToInt64(value);
                    if (enumValueIndex >= ushort.MinValue && enumValueIndex <= ushort.MaxValue)
                    {
                        return AppendValueInternal((ushort)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            case DuckDBType.UnsignedInteger:
                {
                    long enumValueIndex = Convert.ToInt64(value);
                    if (enumValueIndex >= uint.MinValue && enumValueIndex <= uint.MaxValue)
                    {
                        return AppendValueInternal((uint)enumValueIndex, rowIndex);
                    }

                    return false;
                }
            default:
                return false;
        }
    }
}
