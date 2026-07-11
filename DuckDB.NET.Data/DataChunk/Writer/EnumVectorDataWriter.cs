namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class EnumVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private readonly DuckDBType enumType = NativeMethods.LogicalType.DuckDBEnumInternalType(logicalType);

    private readonly uint enumDictionarySize = NativeMethods.LogicalType.DuckDBEnumDictionarySize(logicalType);

    private readonly Dictionary<string, uint> enumValues = new(StringComparer.OrdinalIgnoreCase);

    internal override bool AppendString(string value, ulong rowIndex)
    {
        EnsureEnumValuesInitialized();
        if (enumValues.TryGetValue(value, out var enumValue))
        {
            return AppendEnumDictionaryIndex(enumValue, rowIndex);
        }

        throw new InvalidOperationException($"Failed to write Enum column because the value \"{value}\" is not valid.");
    }

    internal override bool AppendEnum<TEnum>(TEnum value, ulong rowIndex)
    {
        var enumValueType = value.GetType();
        if (enumValueType.IsDefined(typeof(FlagsAttribute), false))
        {
            throw new InvalidOperationException("Failed to write Enum column because [Flags] enums are not supported.");
        }

        var enumName = Enum.GetName(enumValueType, value);
        if (enumName is not null)
        {
            EnsureEnumValuesInitialized();
            if (enumValues.TryGetValue(enumName, out var enumValue))
            {
                return AppendEnumDictionaryIndex(enumValue, rowIndex);
            }
        }

        throw new InvalidOperationException($"Failed to write Enum column because the value \"{value}\" is not valid.");
    }

    private bool AppendEnumDictionaryIndex(ulong dictionaryIndex, ulong rowIndex)
    {
        // The following casts to byte and ushort are safe because we ensure in the constructor that the enumDictionarySize is not too high.
        return enumType switch
        {
            DuckDBType.UnsignedTinyInt => AppendValueInternal((byte)dictionaryIndex, rowIndex),
            DuckDBType.UnsignedSmallInt => AppendValueInternal((ushort)dictionaryIndex, rowIndex),
            DuckDBType.UnsignedInteger => AppendValueInternal((uint)dictionaryIndex, rowIndex),
            _ => throw new InvalidOperationException("Failed to write Enum column because the internal enum type must be utinyint, usmallint, or uinteger."),
        };
    }

    private void EnsureEnumValuesInitialized()
    {
        if (enumValues.Count != 0)
        {
            return;
        }

        for (uint index = 0; index < enumDictionarySize; index++)
        {
            var enumValueName = NativeMethods.LogicalType.DuckDBEnumDictionaryValue(logicalType, index);
            enumValues.Add(enumValueName, index);
        }
    }

}
