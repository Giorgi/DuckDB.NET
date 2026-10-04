namespace DuckDB.NET.Data.DataChunk.Writer;

/// <summary>
/// Shared by the LIST and ARRAY writers, which both write a collection's items into a child vector.
/// Where the items go, and what a row records about them, is up to each subclass.
/// </summary>
internal abstract class CollectionVectorDataWriter : VectorDataWriterBase
{
    private readonly DuckDBLogicalType childType;

    protected unsafe CollectionVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, IntPtr childVector, DuckDBLogicalType childType)
        : base(vector, vectorData, columnType)
    {
        this.childType = childType;
        ItemWriter = VectorDataWriterFactory.CreateWriter(childVector, childType);
    }

    protected VectorDataWriterBase ItemWriter { get; }

    /// <summary>
    /// Writes the collection's items to the child vector, item i at <paramref name="start"/> + i.
    /// </summary>
    protected void WriteCollection(ICollection value, ulong start)
    {
        _ = value switch
        {
            IEnumerable<bool> items => WriteItems(items, start),
            IEnumerable<bool?> items => WriteItems(items, start),

            IEnumerable<sbyte> items => WriteItems(items, start),
            IEnumerable<sbyte?> items => WriteItems(items, start),
            IEnumerable<short> items => WriteItems(items, start),
            IEnumerable<short?> items => WriteItems(items, start),
            IEnumerable<int> items => WriteItems(items, start),
            IEnumerable<int?> items => WriteItems(items, start),
            IEnumerable<long> items => WriteItems(items, start),
            IEnumerable<long?> items => WriteItems(items, start),
            IEnumerable<byte> items => WriteItems(items, start),
            IEnumerable<byte?> items => WriteItems(items, start),
            IEnumerable<ushort> items => WriteItems(items, start),
            IEnumerable<ushort?> items => WriteItems(items, start),
            IEnumerable<uint> items => WriteItems(items, start),
            IEnumerable<uint?> items => WriteItems(items, start),
            IEnumerable<ulong> items => WriteItems(items, start),
            IEnumerable<ulong?> items => WriteItems(items, start),

            IEnumerable<float> items => WriteItems(items, start),
            IEnumerable<float?> items => WriteItems(items, start),
            IEnumerable<double> items => WriteItems(items, start),
            IEnumerable<double?> items => WriteItems(items, start),

            IEnumerable<decimal> items => WriteItems(items, start),
            IEnumerable<decimal?> items => WriteItems(items, start),
            IEnumerable<BigInteger> items => WriteItems(items, start),
            IEnumerable<BigInteger?> items => WriteItems(items, start),

            IEnumerable<string> items => WriteItems(items, start),
            IEnumerable<Guid> items => WriteItems(items, start),
            IEnumerable<Guid?> items => WriteItems(items, start),
            IEnumerable<DateTime> items => WriteItems(items, start),
            IEnumerable<DateTime?> items => WriteItems(items, start),
            IEnumerable<TimeSpan> items => WriteItems(items, start),
            IEnumerable<TimeSpan?> items => WriteItems(items, start),
            IEnumerable<DuckDBDateOnly> items => WriteItems(items, start),
            IEnumerable<DuckDBDateOnly?> items => WriteItems(items, start),
            IEnumerable<DuckDBTimeOnly> items => WriteItems(items, start),
            IEnumerable<DuckDBTimeOnly?> items => WriteItems(items, start),
            IEnumerable<DateOnly> items => WriteItems(items, start),
            IEnumerable<DateOnly?> items => WriteItems(items, start),
            IEnumerable<TimeOnly> items => WriteItems(items, start),
            IEnumerable<TimeOnly?> items => WriteItems(items, start),
            IEnumerable<DateTimeOffset> items => WriteItems(items, start),
            IEnumerable<DateTimeOffset?> items => WriteItems(items, start),
            IEnumerable<object> items => WriteItems(items, start),

            _ => WriteItemsFallback(value, start),
        };
    }

    public override void Dispose()
    {
        ItemWriter.Dispose();
        childType.Dispose();
    }

    private int WriteItems<T>(IEnumerable<T> items, ulong start)
    {
        var index = 0ul;

        foreach (var item in items)
        {
            ItemWriter.WriteValue(item, start + (index++));
        }

        return 0;
    }

    private int WriteItemsFallback(IEnumerable items, ulong start)
    {
        var index = 0ul;

        foreach (var item in items)
        {
            ItemWriter.WriteValue(item, start + (index++));
        }

        return 0;
    }
}
