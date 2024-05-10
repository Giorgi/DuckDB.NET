using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class StructVectorDataReader : VectorDataReaderBase
{
    private static readonly ConcurrentDictionary<Type, TypeDetails> TypeCache = new();
    private readonly Dictionary<string, VectorDataReaderBase> structDataReaders;

    internal unsafe StructVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        var memberCount = NativeMethods.LogicalType.DuckDBStructTypeChildCount(logicalType);
        structDataReaders = new Dictionary<string, VectorDataReaderBase>(StringComparer.OrdinalIgnoreCase);

        for (int index = 0; index < memberCount; index++)
        {
            var name = NativeMethods.LogicalType.DuckDBStructTypeChildName(logicalType, index).ToManagedString();
            var childVector = NativeMethods.Vectors.DuckDBStructVectorGetChild(vector, index);
            
            using var childType = NativeMethods.LogicalType.DuckDBStructTypeChildType(logicalType, index);
            structDataReaders[name] = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
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

    public override void Dispose()
    {
        foreach (var reader in structDataReaders)
        {
            reader.Value.Dispose();
        }

        base.Dispose();
    }
}