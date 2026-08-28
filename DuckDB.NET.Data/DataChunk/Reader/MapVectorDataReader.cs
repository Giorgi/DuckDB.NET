namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class MapVectorDataReader : VectorDataReaderBase
{
    private readonly VectorDataReaderBase keyReader;
    private readonly VectorDataReaderBase valueReader;

    internal unsafe MapVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, DuckDBLogicalType logicalColumnType, string columnName) 
                    : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var keyTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeKeyType(logicalColumnType);
        using var valueTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeValueType(logicalColumnType);

        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);

        var keyVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 0);
        var valueVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 1);

        keyReader = VectorDataReaderFactory.CreateReader(keyVector, keyTypeLogical, columnName);
        valueReader = VectorDataReaderFactory.CreateReader(valueVector, valueTypeLogical, columnName);
    }

    protected override Type GetColumnType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ClrType, valueReader.ClrType);
    }

    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ProviderSpecificClrType, valueReader.ProviderSpecificClrType);
    }

    internal override unsafe object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Map)
        {
            return base.GetValue(offset, targetType);
        }

        if (Activator.CreateInstance(targetType) is not IDictionary instance)
        {
            throw new InvalidOperationException($"Cannot read Map column {ColumnName} in a non-dictionary type");
        }

        var arguments = targetType.GetGenericArguments();

        var keyTargetType = keyReader.ClrType;
        var valueTargetType = valueReader.ClrType;
        var allowsNullValues = false;

        if (arguments.Length == 2)
        {
            // A Dictionary<..., object> expresses no element-type preference: keep the child's natural ClrType.
            arguments[0].AllowsNullValue(out _, out var underlyingKeyType);
            keyTargetType = underlyingKeyType ?? (arguments[0] == typeof(object) ? keyReader.ClrType : arguments[0]);

            allowsNullValues = arguments[1].AllowsNullValue(out _, out var underlyingValueType);
            valueTargetType = underlyingValueType ?? (arguments[1] == typeof(object) ? valueReader.ClrType : arguments[1]);
        }

        var listData = (DuckDBListEntry*)DataPointer + offset;

        for (ulong i = 0; i < listData->Length; i++)
        {
            var childOffset = i + listData->Offset;

            var key = keyReader.IsValid(childOffset) ? keyReader.GetValue(childOffset, keyTargetType) : null!;
            var value = valueReader.IsValid(childOffset) ? valueReader.GetValue(childOffset, valueTargetType) : null;

            if (allowsNullValues || value != null)
            {
                instance.Add(key, value);
            }
            else
            {
                throw new InvalidCastException($"The Map in column {ColumnName} contains null value but dictionary does not allow null values");
            }
        }

        return instance;
    }

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);
        var keyVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 0);
        var valueVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(childVector, 1);
        keyReader.Reset(keyVector);
        valueReader.Reset(valueVector);
    }

    public override void Dispose()
    {
        keyReader.Dispose();
        valueReader.Dispose();
        base.Dispose();
    }
}