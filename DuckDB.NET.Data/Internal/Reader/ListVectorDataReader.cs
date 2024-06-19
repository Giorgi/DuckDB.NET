using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Collections.Generic;
#if NET8_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class ListVectorDataReader : VectorDataReaderBase
{
    private readonly ulong arraySize;
    private readonly VectorDataReaderBase listDataReader;

    public bool IsList => DuckDBType == DuckDBType.List;

    internal unsafe ListVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        using var childType = IsList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) : NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);

        var childVector = IsList ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector) : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);

        arraySize = IsList ? 0 : (ulong)NativeMethods.LogicalType.DuckDBArrayVectorGetSize(logicalType);
        listDataReader = VectorDataReaderFactory.CreateReader(childVector, childType, columnName);
    }
#if NET8_0_OR_GREATER
    [return:DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
#endif
    protected override Type GetColumnType()
    {
        return typeof(List<>).MakeGenericType(listDataReader.ClrType);
    }

#if NET8_0_OR_GREATER
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)]
#endif
    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(List<>).MakeGenericType(listDataReader.ProviderSpecificClrType);
    }

    internal override unsafe object GetValue(ulong offset, Type targetType)
    {
        switch (DuckDBType)
        {
            case DuckDBType.List:
                {
                    var listData = (DuckDBListEntry*)DataPointer + offset;

                    return GetList(targetType, listData->Offset, listData->Length);
                }
            case DuckDBType.Array:
                return GetList(targetType, offset * arraySize, arraySize);
            default:
                return base.GetValue(offset, targetType);
        }
    }

    private unsafe object GetList(Type returnType, ulong listOffset, ulong length)
    {
        var listType = returnType.GetGenericArguments()[0];

        var allowNulls = listType.AllowsNullValue(out var _, out var nullableType);

        var list = CreatorCache.GetCreator(returnType)() as IList
                   ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        //Special case for specific types to avoid boxing
        return list switch
        {
            List<int> theList => BuildList<int>(theList),
            List<int?> theList => BuildList<int?>(theList),
            List<float> theList => BuildList<float>(theList),
            List<float?> theList => BuildList<float?>(theList),
            List<double> theList => BuildList<double>(theList),
            List<double?> theList => BuildList<double?>(theList),
            List<decimal> theList => BuildList<decimal>(theList),
            List<decimal?> theList => BuildList<decimal?>(theList),
            _ => BuildListCommon(list, nullableType ?? listType)
        };

        List<T> BuildList<T>(List<T> result)
        {
            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;
                if (listDataReader.IsValid(childOffset))
                {
                    var item = listDataReader.GetValue<T>(childOffset);
                    result.Add(item);
                }
                else
                {
                    result.Add(allowNulls ? default! : throw new InvalidCastException("The list contains null value"));
                }
            }
            return result;
        }

        IList BuildListCommon(IList result, Type targetType)
        {
            for (ulong i = 0; i < length; i++)
            {
                var childOffset = listOffset + i;
                if (listDataReader.IsValid(childOffset))
                {
                    var item = listDataReader.GetValue(childOffset, targetType);
                    result.Add(item);
                }
                else
                {
                    result.Add(allowNulls ? null : throw new InvalidCastException("The list contains null value"));
                }
            }
            return result;
        }
    }

    public override void Dispose()
    {
        listDataReader.Dispose();
        base.Dispose();
    }
}