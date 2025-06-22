using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using DuckDB.NET.Data.Common;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class ListVectorDataWriter : VectorDataWriterBase
{
    private ulong offset = 0;
    private readonly ulong arraySize;
    private readonly DuckDBLogicalType childType;
    private readonly VectorDataWriterBase listItemWriter;

    private bool IsList => ColumnType == DuckDBType.List;
    private ulong vectorReservedSize = DuckDBGlobalData.VectorSize;

    public ListVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, DuckDBLogicalType logicalType) : base(vector, vectorData, columnType)
    {
        childType = IsList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) : NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);
        var childVector = IsList ? NativeMethods.Vectors.DuckDBListVectorGetChild(vector) : NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector);

        arraySize = IsList ? 0 : (ulong)NativeMethods.LogicalType.DuckDBArrayVectorGetSize(logicalType);
        listItemWriter = VectorDataWriterFactory.CreateWriter(childVector, childType);
    }

    internal override bool AppendCollection(ICollection value, ulong rowIndex)
    {
        var count = (ulong)value.Count;

        ResizeVector(rowIndex % DuckDBGlobalData.VectorSize, count);

        _ = value switch
        {
            IEnumerable<bool> items => WriteItems(items),
            IEnumerable<bool?> items => WriteItems(items),

            IEnumerable<sbyte> items => WriteItems(items),
            IEnumerable<sbyte?> items => WriteItems(items),
            IEnumerable<short> items => WriteItems(items),
            IEnumerable<short?> items => WriteItems(items),
            IEnumerable<int> items => WriteItems(items),
            IEnumerable<int?> items => WriteItems(items),
            IEnumerable<long> items => WriteItems(items),
            IEnumerable<long?> items => WriteItems(items),
            IEnumerable<byte> items => WriteItems(items),
            IEnumerable<byte?> items => WriteItems(items),
            IEnumerable<ushort> items => WriteItems(items),
            IEnumerable<ushort?> items => WriteItems(items),
            IEnumerable<uint> items => WriteItems(items),
            IEnumerable<uint?> items => WriteItems(items),
            IEnumerable<ulong> items => WriteItems(items),
            IEnumerable<ulong?> items => WriteItems(items),

            IEnumerable<float> items => WriteItems(items),
            IEnumerable<float?> items => WriteItems(items),
            IEnumerable<double> items => WriteItems(items),
            IEnumerable<double?> items => WriteItems(items),

            IEnumerable<decimal> items => WriteItems(items),
            IEnumerable<decimal?> items => WriteItems(items),
            IEnumerable<BigInteger> items => WriteItems(items),
            IEnumerable<BigInteger?> items => WriteItems(items),

            IEnumerable<string> items => WriteItems(items),
            IEnumerable<Guid> items => WriteItems(items),
            IEnumerable<Guid?> items => WriteItems(items),
            IEnumerable<DateTime> items => WriteItems(items),
            IEnumerable<DateTime?> items => WriteItems(items),
            IEnumerable<TimeSpan> items => WriteItems(items),
            IEnumerable<TimeSpan?> items => WriteItems(items),
            IEnumerable<DuckDBDateOnly> items => WriteItems(items),
            IEnumerable<DuckDBDateOnly?> items => WriteItems(items),
            IEnumerable<DuckDBTimeOnly> items => WriteItems(items),
            IEnumerable<DuckDBTimeOnly?> items => WriteItems(items),
#if NET6_0_OR_GREATER
            IEnumerable<DateOnly> items => WriteItems(items),
            IEnumerable<DateOnly?> items => WriteItems(items),
            IEnumerable<TimeOnly> items => WriteItems(items),
            IEnumerable<TimeOnly?> items => WriteItems(items),
#endif
            IEnumerable<DateTimeOffset> items => WriteItems(items),
            IEnumerable<DateTimeOffset?> items => WriteItems(items),
            IEnumerable<object> items => WriteItems(items),

            _ => WriteItemsFallback(value),
        };

        var duckDBListEntry = new DuckDBListEntry(offset, count);
        var result = !IsList || AppendValueInternal(duckDBListEntry, rowIndex);

        offset += count;

        return result;

        int WriteItems<T>(IEnumerable<T> items)
        {
            if (IsList == false && count != arraySize)
            {
                throw new InvalidOperationException($"Column has Array size of {arraySize} but the specified value has size of {count}");
            }

            var index = 0ul;

            foreach (var item in items)
            {
                listItemWriter.WriteValue(item, offset + (index++));
            }

            return 0;
        }

        int WriteItemsFallback(IEnumerable items)
        {
            if (IsList == false && count != arraySize)
            {
                throw new InvalidOperationException($"Column has Array size of {arraySize} but the specified value has size of {count}");
            }

            var index = 0ul;

            foreach (var item in items)
            {
                listItemWriter.WriteValue(item, offset + (index++));
            }

            return 0;
        }
    }

    private void ResizeVector(ulong rowIndex, ulong count)
    {
        //If writing to a list column we need to make sure that enough space is allocated. Not needed for Arrays as DuckDB does it for us.
        if (!IsList || offset + count <= vectorReservedSize) return;

        var factor = 2d;

        if (rowIndex > DuckDBGlobalData.VectorSize * 0.25 && rowIndex < DuckDBGlobalData.VectorSize * 0.5)
        {
            factor = 1.75;
        }

        if (rowIndex > DuckDBGlobalData.VectorSize * 0.5 && rowIndex < DuckDBGlobalData.VectorSize * 0.75)
        {
            factor = 1.5;
        }

        if (rowIndex > DuckDBGlobalData.VectorSize * 0.75)
        {
            factor = 1.25;
        }

        vectorReservedSize = (ulong)Math.Max(vectorReservedSize * factor, offset + count);
        var state = NativeMethods.Vectors.DuckDBListVectorReserve(Vector, vectorReservedSize);

        if (!state.IsSuccess())
        {
            throw new DuckDBException($"Failed to reserve {vectorReservedSize} for the list vector");
        }

        listItemWriter.InitializeWriter();
    }

    public override void Dispose()
    {
        listItemWriter.Dispose();
        childType.Dispose();
    }
}