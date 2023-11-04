using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class StructVectorDataReader : VectorDataReaderBase
{
    private static readonly ConcurrentDictionary<Type, TypeDetails> TypeCache = new();
    private readonly Dictionary<string, VectorDataReaderBase> structDataReaders;

    internal unsafe StructVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
        structDataReaders = new Dictionary<string, VectorDataReaderBase>(StringComparer.OrdinalIgnoreCase);

        for (int index = 0; index < memberCount; index++)
        {
            var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index).ToManagedString();
            var childVector = NativeMethods.DataChunks.DuckDBStructVectorGetChild(vector, index);

            var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
            var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

            using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
            var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

            structDataReaders[name] = VectorDataReaderFactory.CreateReader(childVector, childVectorData, childVectorValidity, type, columnName);
        }
    }

    internal override object GetValue(ulong offset, Type? targetType = null)
    {
        if (DuckDBType == DuckDBType.Struct)
        {
            return GetStruct(offset, targetType ?? ClrType);
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

                var isNullable = !propertyInfo.PropertyType.IsValueType || Nullable.GetUnderlyingType(propertyInfo.PropertyType) != null;

                var instanceParam = Expression.Parameter(typeof(object));
                var argumentParam = Expression.Parameter(typeof(object));

                var setAction = Expression.Lambda<Action<object, object>>(
                    Expression.Call(Expression.Convert(instanceParam, type), propertyInfo.SetMethod, Expression.Convert(argumentParam, propertyInfo.PropertyType)),
                    instanceParam, argumentParam
                ).Compile();

                details.Properties.Add(propertyInfo.Name, new PropertyDetails(propertyInfo.PropertyType, isNullable, setAction));
            }

            return details;
        });

        foreach (var properties in typeDetails.Properties)
        {
            structDataReaders.TryGetValue(properties.Key, out var reader);
            var isNullable = properties.Value.Nullable;

            if (reader == null)
            {
                if (!isNullable)
                {
                    throw new NullReferenceException($"Property '{properties.Key}' not found in struct");
                }

                continue;
            }

            if (reader.IsValid(offset))
            {
                var value = reader.GetValue(offset, properties.Value.PropertyType);
                properties.Value.Setter(result!, value);
            }
            else
            {
                if (!isNullable)
                {
                    throw new NullReferenceException($"Property '{properties.Key}' is not nullable but struct contains null value");
                }
            }
        }

        return result!;
    }

    public override void Dispose()
    {
        foreach (var reader in structDataReaders)
        {
            reader.Value.Dispose();
        }

        base.Dispose();
    }
}