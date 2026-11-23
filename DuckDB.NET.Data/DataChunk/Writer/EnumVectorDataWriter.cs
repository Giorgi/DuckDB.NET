using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Writer;

// An ENUM's entries are its type's parameters, one per name, and the storage of the index follows from their number
// as V2 states in its contract: uint8 up to 255 entries, uint16 up to 65535, uint32 beyond.
internal sealed unsafe class EnumVectorDataWriter : VectorDataWriterBase
{
    private readonly DuckDBType enumType;

    private readonly uint enumDictionarySize;

    // A copy, to read the names from on the first string.
    private readonly DuckDBV2LogicalType logicalType;

    private readonly Dictionary<string, uint> enumValues = [];

    public EnumVectorDataWriter(IntPtr vector, void* vectorData, _duckdb_v2_logical_type* type, DuckDBType columnType) : base(vector, vectorData, columnType)
    {
        enumDictionarySize = (uint)DuckDBV2LogicalTypes.GetParamCount(type);
        enumType = enumDictionarySize switch
        {
            <= byte.MaxValue => DuckDBType.UnsignedTinyInt,
            <= ushort.MaxValue => DuckDBType.UnsignedSmallInt,
            _ => DuckDBType.UnsignedInteger,
        };
        logicalType = DuckDBV2LogicalTypes.Copy(type);
    }

    internal override bool AppendString(string value, ulong rowIndex)
    {
        if (enumValues.Count == 0)
        {
            using var lease = logicalType.Acquire();

            for (uint index = 0; index < enumDictionarySize; index++)
            {
                var enumValueName = DuckDBV2LogicalTypes.GetStringParam(lease.Pointer, index);
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

    internal override bool AppendEnum(ulong enumValue, ulong rowIndex)
    {
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

    public override void Dispose()
    {
        logicalType.Dispose();
        base.Dispose();
    }

}
