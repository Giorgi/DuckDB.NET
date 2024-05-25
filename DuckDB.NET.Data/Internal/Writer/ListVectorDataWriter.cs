using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class ListVectorDataWriter : VectorDataWriterBase
{
    private ulong offset = 0;
    private readonly VectorDataWriterBase listItemWriter;

    public ListVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, DuckDBLogicalType logicalType) : base(vector, vectorData, columnType)
    {
        using var childType = NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType);
        var childVector = NativeMethods.Vectors.DuckDBListVectorGetChild(vector);
        listItemWriter = VectorDataWriterFactory.CreateWriter(childVector, childType);
    }

    internal override bool AppendCollection(IList value, int rowIndex)
    {
        _ = value switch
        {
            IEnumerable<bool> items => WriteItems(items),

            IEnumerable<sbyte> items => WriteItems(items),
            IEnumerable<short> items => WriteItems(items),
            IEnumerable<int> items => WriteItems(items),
            IEnumerable<long> items => WriteItems(items),
            IEnumerable<byte> items => WriteItems(items),
            IEnumerable<ushort> items => WriteItems(items),
            IEnumerable<uint> items => WriteItems(items),
            IEnumerable<ulong> items => WriteItems(items),
            
            IEnumerable<decimal> items => WriteItems(items),
            IEnumerable<BigInteger> items => WriteItems(items),
            
            IEnumerable<string> items => WriteItems(items),
            IEnumerable<Guid> items => WriteItems(items),
            IEnumerable<DateTime> items => WriteItems(items),
            IEnumerable<TimeSpan> items => WriteItems(items),
            IEnumerable<DuckDBDateOnly> items => WriteItems(items),
            IEnumerable<DuckDBTimeOnly> items => WriteItems(items),
#if NET6_0_OR_GREATER
            IEnumerable<DateOnly> items => WriteItems(items),
            IEnumerable<TimeOnly> items => WriteItems(items),
#endif
            IEnumerable<DateTimeOffset> items => WriteItems(items),
            
            _ => WriteItems<object>((IEnumerable<object>)value)
        };

        var result = AppendValueInternal(new DuckDBListEntry(offset, (ulong)value.Count), rowIndex);

        offset += (ulong)value.Count;

        return result;

        int WriteItems<T>(IEnumerable<T> items)
        {
            var index = 0;

            foreach (var item in items)
            {
                listItemWriter.AppendValue(item, (int)offset + (index++));
            }

            return 0;
        }
    }
}