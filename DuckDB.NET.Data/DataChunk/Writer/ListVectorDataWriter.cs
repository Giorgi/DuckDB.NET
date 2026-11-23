using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;
using DuckDB.NET.Data.Common;

namespace DuckDB.NET.Data.DataChunk.Writer;

// No InitializeWriter override, unlike ArrayVectorDataWriter: when a list around this one grows, DuckDB
// moves this list's entries but not its items (Vector::FindResizeInfos stops at list buffers), and
// ResizeVector refreshes ItemWriter itself when this list's own items move.
internal sealed class ListVectorDataWriter : CollectionVectorDataWriter
{
    private ulong offset = 0;
    private ulong vectorReservedSize = DuckDBGlobalData.VectorSize;

    public unsafe ListVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType, _duckdb_v2_logical_type* logicalType)
        : base(vector, vectorData, columnType, logicalType)
    {
    }

    internal override bool AppendCollection(ICollection value, ulong rowIndex)
    {
        var count = (ulong)value.Count;

        ResizeVector(rowIndex % DuckDBGlobalData.VectorSize, count);

        // Each list entry records where its items start, so the items can go at the running offset.
        var start = offset;

        WriteCollection(value, start);

        var result = AppendValueInternal(new DuckDBListEntry(start, count), rowIndex);

        offset += count;

        // V2 sizes the items' own vector, where V1 set the size on the list (duckdb_list_vector_set_size).
        unsafe
        {
            DuckDBV2Vectors.SetSize((_duckdb_v2_vector*)ItemWriter.Vector, offset);
        }

        return result;
    }

    private void ResizeVector(ulong rowIndex, ulong count)
    {
        //Make sure that enough space is allocated for the list's items. Arrays don't need this, as DuckDB sizes their items for us.
        if (offset + count <= vectorReservedSize) return;

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

        // Setting the size reserves room for it; the size that counts is set after each list. The items' vector can
        // move, so its writer fetches its pointers again.
        unsafe
        {
            DuckDBV2Vectors.SetSize((_duckdb_v2_vector*)ItemWriter.Vector, vectorReservedSize);
        }

        ItemWriter.InitializeWriter();
    }
}
