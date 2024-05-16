using System;
using System.Collections;
using System.Collections.Generic;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class ListVectorDataWriter : VectorDataWriterBase
{
    private ulong offset = 0;
    private readonly VectorDataWriterBase listDataWriter;

    public ListVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, DuckDBLogicalType logicalType) : base(vector, vectorData, columnType)
    {
        using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);
        listDataWriter = VectorDataWriterFactory.CreateWriter(childVector, childType);
    }

    internal override bool AppendCollection(IList value, int rowIndex)
    {
        var index = 0;

        foreach (var item in value)
        {
            listDataWriter.AppendValue(item, (int)offset + (index++));
        }

        var result = AppendValueInternal(new DuckDBListEntry(offset, (ulong)value.Count), rowIndex);

        offset += (ulong)value.Count;

        return result;
    }
}