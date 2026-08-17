using System.Collections.Concurrent;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class MapVectorDataReader : VectorDataReaderBase
{
    private static readonly ConcurrentDictionary<Type, IMapMaterializer> Materializers = new();

    private readonly VectorDataReaderBase keyReader;
    private readonly VectorDataReaderBase valueReader;
    private Type? cachedTargetType;
    private IMapMaterializer? cachedMaterializer;

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

        var listData = (DuckDBListEntry*)DataPointer + offset;
        var materializer = GetMaterializer(targetType);

        if (materializer != null)
        {
            return materializer.Materialize(keyReader, valueReader, listData->Offset, listData->Length, ColumnName);
        }

        if (Activator.CreateInstance(targetType) is not IDictionary instance)
        {
            throw new InvalidOperationException($"Cannot read Map column {ColumnName} in a non-dictionary type");
        }

        var arguments = targetType.GetGenericArguments();
        var allowsNullValues = arguments.Length == 2 && arguments[1].AllowsNullValue(out _, out _);

        for (ulong i = 0; i < listData->Length; i++)
        {
            var childOffset = i + listData->Offset;

            var key = keyReader.GetValue(childOffset);
            var value = valueReader.IsValid(childOffset) ? valueReader.GetValue(childOffset) : null;

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

    private IMapMaterializer? GetMaterializer(Type targetType)
    {
        if (targetType == cachedTargetType)
        {
            return cachedMaterializer;
        }

        cachedTargetType = targetType;
        cachedMaterializer = null;

        if (!targetType.IsGenericType || targetType.GetGenericTypeDefinition() != typeof(Dictionary<,>))
        {
            return null;
        }

        var arguments = targetType.GetGenericArguments();
        if (!CanReadDirectly(keyReader, arguments[0]) || !CanReadDirectly(valueReader, arguments[1]))
        {
            return null;
        }

        cachedMaterializer = Materializers.GetOrAdd(targetType, static type => CreateMaterializer(type));
        return cachedMaterializer;
    }

    private static bool CanReadDirectly(VectorDataReaderBase reader, Type targetType)
    {
        var valueType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        return valueType == reader.ClrType || valueType == reader.ProviderSpecificClrType;
    }

    private static IMapMaterializer CreateMaterializer(Type targetType)
    {
        var materializerType = typeof(MapMaterializer<,>).MakeGenericType(targetType.GetGenericArguments());
        return (IMapMaterializer)Activator.CreateInstance(materializerType)!;
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

    private interface IMapMaterializer
    {
        object Materialize(VectorDataReaderBase keys, VectorDataReaderBase values, ulong offset, ulong length, string columnName);
    }

    private sealed class MapMaterializer<TKey, TValue> : IMapMaterializer where TKey : notnull
    {
        private static readonly bool AllowsNullValues = typeof(TValue).AllowsNullValue(out _, out _);

        public object Materialize(VectorDataReaderBase keys, VectorDataReaderBase values, ulong offset, ulong length, string columnName)
        {
            var result = new Dictionary<TKey, TValue>(checked((int)length));

            for (ulong i = 0; i < length; i++)
            {
                var childOffset = offset + i;
                var key = Read<TKey>(keys, childOffset);

                if (values.IsValid(childOffset))
                {
                    result.Add(key, Read<TValue>(values, childOffset));
                }
                else if (AllowsNullValues)
                {
                    result.Add(key, default!);
                }
                else
                {
                    throw new InvalidCastException($"The Map in column {columnName} contains null value but dictionary does not allow null values");
                }
            }

            return result;
        }

        private static T Read<T>(VectorDataReaderBase reader, ulong offset)
        {
            // Object-valued readers need their natural non-generic path. Concrete value types use
            // the generic path so they can flow into Dictionary<TKey, TValue> without boxing.
            return typeof(T) == typeof(object)
                ? (T)reader.GetValue(offset)
                : reader.GetValueStrict<T>(offset);
        }
    }
}
