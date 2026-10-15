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

    // The properties of the type GetStruct was asked for last, each with the reader of its field. A column is read as
    // the same type row after row, so each property is matched with its field once and not for every row.
    private Type? pairedType;
    private PropertyReader[] propertyReaders = [];

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

        if (returnType != pairedType)
        {
            propertyReaders = PairPropertiesWithReaders(returnType);
            pairedType = returnType;
        }

        foreach (var (name, property, reader) in propertyReaders)
        {
            if (reader.IsValid(offset))
            {
                var value = reader.GetValue(offset, property.NullableType ?? property.PropertyType);
                property.Setter(result!, value);
            }
            else
            {
                if (!property.Nullable)
                {
                    throw new InvalidCastException($"Property '{name}' is not nullable but struct contains null value");
                }
            }
        }

        return result!;
    }

    // In the order of the properties of the type. A property with no field of its name is left as it is.
    private PropertyReader[] PairPropertiesWithReaders(Type returnType)
    {
        // A static lambda: one that used returnType allocated a closure and a delegate for every call.
        var typeDetails = TypeCache.GetOrAdd(returnType, static type =>
        {
            var propertyInfos = type.GetProperties();
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

        var pairs = new List<PropertyReader>(typeDetails.Properties.Count);

        foreach (var property in typeDetails.Properties)
        {
            if (structDataReaders.TryGetValue(property.Key, out var reader))
            {
                pairs.Add(new PropertyReader(property.Key, property.Value, reader));
            }
        }

        return [.. pairs];
    }

    private readonly record struct PropertyReader(string Name, PropertyDetails Property, VectorDataReaderBase Reader);

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