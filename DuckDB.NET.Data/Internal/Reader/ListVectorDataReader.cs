using System;
using System.Collections;
using System.Collections.Generic;

namespace DuckDB.NET.Data.Internal.Reader;

internal class ListVectorDataReader : VectorDataReader
{
    private readonly VectorDataReader listDataReader;

    internal unsafe ListVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(vector, dataPointer, validityMaskPointer, columnType)
    {
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);

        var type = NativeMethods.LogicalType.DuckDBGetTypeId(childType);

        var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);

        var childVectorData = NativeMethods.DataChunks.DuckDBVectorGetData(childVector);
        var childVectorValidity = NativeMethods.DataChunks.DuckDBVectorGetValidity(childVector);

        listDataReader = VectorDataReaderFactory.CreateReader(childVector, childVectorData, childVectorValidity, type);
        ClrType = typeof(List<>).MakeGenericType(listDataReader.ClrType);
    }

    internal override unsafe object GetList(ulong offset, Type returnType)
    {
        var listData = (DuckDBListEntry*)DataPointer + offset;

        var listType = returnType.GetGenericArguments()[0];

        var nullableType = Nullable.GetUnderlyingType(listType);
        var allowNulls = !listType.IsValueType || nullableType != null;

        var list = Activator.CreateInstance(returnType) as IList
                   ?? throw new ArgumentException($"The type '{returnType.Name}' specified in parameter {nameof(returnType)} cannot be instantiated as an IList.");

        var targetType = nullableType ?? listType;

        for (ulong i = 0; i < listData->Length; i++)
        {
            var childOffset = i + listData->Offset;
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
                    throw new NullReferenceException("The list contains null value");
                }
            }
        }

        return list;
    }

    public override void Dispose()
    {
        listDataReader.Dispose();
        base.Dispose();
    }
}