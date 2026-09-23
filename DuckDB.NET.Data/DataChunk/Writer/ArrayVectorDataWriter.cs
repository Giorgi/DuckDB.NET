namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed class ArrayVectorDataWriter : CollectionVectorDataWriter
{
    private readonly ulong arraySize;

    public unsafe ArrayVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, DuckDBLogicalType logicalType)
        : base(vector, vectorData, columnType, NativeMethods.Vectors.DuckDBArrayVectorGetChild(vector), NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType))
    {
        arraySize = (ulong)NativeMethods.LogicalType.DuckDBArrayVectorGetSize(logicalType);
    }

    internal override bool AppendCollection(ICollection value, ulong rowIndex)
    {
        var count = (ulong)value.Count;

        if (count != arraySize)
        {
            throw new InvalidOperationException($"Column has Array size of {arraySize} but the specified value has size of {count}");
        }

        // An ARRAY records nothing about where its items are: DuckDB reads row r's items at
        // r * arraySize, so the position must come from the row. A running offset would fall behind
        // after a NULL array, which never reaches this method.
        WriteCollection(value, rowIndex * arraySize);

        return true;
    }

    // An array's items are sized from its capacity, so when a list around this array grows, DuckDB
    // moves them too (Vector::Resize walks into array children, but stops at list items). Refresh the
    // item writer as well, which repeats all the way down through nested arrays.
    internal override void InitializeWriter()
    {
        base.InitializeWriter();
        ItemWriter.InitializeWriter();
    }

    public override void WriteNull(ulong rowIndex)
    {
        base.WriteNull(rowIndex);

        // DuckDB's appender hides a NULL array's items only in its own copy of the validity. For LIST
        // items it reads the list headers through the vector's own validity
        // (ListVector::GetConsecutiveChildListInfo), so unmarked items would hand it garbage offsets and
        // lengths. Marking every item NULL, recursively for nested arrays, keeps them skipped.
        var start = rowIndex * arraySize;

        for (ulong index = 0; index < arraySize; index++)
        {
            ItemWriter.WriteNull(start + index);
        }
    }
}
