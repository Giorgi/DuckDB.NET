using System.Collections.Concurrent;
using System.Linq.Expressions;
using DuckDB.NET.Data.Common;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class StructVectorDataReader : VectorDataReaderBase
{
    private static readonly ConcurrentDictionary<Type, TypeDetails> TypeCache = new();
    // TODO: Replace with OrderedDictionary<string, VectorDataReaderBase> when .NET 8 support is dropped.
    // The parallel array exists only to guarantee field-ordinal iteration in Reset/Dispose.
    private readonly Dictionary<string, VectorDataReaderBase> structDataReaders;
    private readonly VectorDataReaderBase[] orderedReaders;

    internal unsafe StructVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
        structDataReaders = new Dictionary<string, VectorDataReaderBase>((int)memberCount, StringComparer.OrdinalIgnoreCase);
        orderedReaders = new VectorDataReaderBase[memberCount];

        for (int index = 0; index < memberCount; index++)
        {
            var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index);
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);

            using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
            var reader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
            structDataReaders[name] = reader;
            orderedReaders[index] = reader;
        }
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Struct)
        {
            return GetStruct(offset, targetType);
        }

        return base.GetValue(offset, targetType);
    }

    private object GetStruct(ulong offset, Type returnType)
    {
        var result = Activator.CreateInstance(returnType);

        if (result is Dictionary<string, object?> dictionary)
        {
            foreach (var reader in structDataReaders)
            {
                var value = reader.Value.IsValid(offset) ? reader.Value.GetValue(offset) : null;
                dictionary.Add(reader.Key, value);
            }

            return result;
        }

        var typeDetails = TypeCache.GetOrAdd(returnType, type =>
        {
            var propertyInfos = returnType.GetProperties();
            var details = new TypeDetails();

            foreach (var propertyInfo in propertyInfos)
            {
                if (propertyInfo.SetMethod == null)
                {
                    continue;
                }

                var isNullable = propertyInfo.PropertyType.AllowsNullValue(out var isNullableValueType, out var underlyingType);

                var instanceParam = Expression.Parameter(typeof(object));
                var argumentParam = Expression.Parameter(typeof(object));

                var setAction = Expression.Lambda<Action<object, object>>(
                    Expression.Call(Expression.Convert(instanceParam, type), propertyInfo.SetMethod, Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                    instanceParam, argumentParam
                ).Compile();

                details.Properties.Add(propertyInfo.Name, new PropertyDetails(propertyInfo.PropertyType, isNullable, isNullableValueType, underlyingType, setAction));
            }

            return details;
        });

        foreach (var property in typeDetails.Properties)
        {
            structDataReaders.TryGetValue(property.Key, out var reader);
            var isNullable = property.Value.Nullable;

            if (reader == null)
            {
                //if (!isNullable)
                //{
                //    throw new NullReferenceException($"Property '{properties.Key}' not found in struct");
                //}

                continue;
            }

            if (reader.IsValid(offset))
            {
                var value = reader.GetValue(offset, property.Value.NullableType ?? property.Value.PropertyType);
                property.Value.Setter(result!, value);
            }
            else
            {
                if (!isNullable)
                {
                    throw new InvalidCastException($"Property '{property.Key}' is not nullable but struct contains null value");
                }
            }
        }

        return result!;
    }

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        for (int index = 0; index < orderedReaders.Length; index++)
        {
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);
            orderedReaders[index].Reset(childVector);
        }
    }

    public override void Dispose()
    {
        foreach (var reader in orderedReaders)
        {
            reader.Dispose();
        }

        base.Dispose();
    }
}