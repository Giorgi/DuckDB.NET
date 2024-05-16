using System;
using System.Collections;
using System.Collections.Generic;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

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

    protected override Type GetColumnType()
    {
        return typeof(List<>).MakeGenericType(listDataReader.ClrType);
    }

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

        var list = Activator.CreateInstance(returnType) as IList
                   ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        //Special case for specific types to avoid boxing
        switch (list)
        {
            case List<int> theList:
                return BuildList<int>(theList);
            case List<int?> theList:
                return BuildList<int?>(theList);
            case List<float> theList:
                return BuildList<float>(theList);
            case List<float?> theList:
                return BuildList<float?>(theList);
            case List<double> theList:
                return BuildList<double>(theList);
            case List<double?> theList:
                return BuildList<double?>(theList);
            case List<decimal> theList:
                return BuildList<decimal>(theList);
            case List<decimal?> theList:
                return BuildList<decimal?>(theList);
        }

        var targetType = nullableType ?? listType;

        for (ulong i = 0; i < length; i++)
        {
            var childOffset = listOffset + i;
            if (listDataReader.IsValid(childOffset))
            {
                var item = listDataReader.GetValue(childOffset, targetType);
                list.Add(item);
            }
            else
            {
                if (allowNulls)
                {
                    list.Add(null);
                }
                else
                {
                    throw new InvalidCastException("The list contains null value");
                }
            }
        }

        return list;

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
                    if (allowNulls)
                    {
                        result.Add(default!);
                    }
                    else
                    {
                        throw new InvalidCastException("The list contains null value");
                    }
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