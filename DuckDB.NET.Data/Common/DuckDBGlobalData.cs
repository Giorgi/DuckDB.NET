namespace DuckDB.NET.Data.Common;

internal static class DuckDBGlobalData
{
    /// <summary>
    /// DuckDB's standard vector size (STANDARD_VECTOR_SIZE), which the release builds use. V2 has no call for it: the
    /// code that fills chunks takes their capacity from the chunk (<c>data_chunk_get_capacity</c>), and this is only a
    /// sizing hint for the list writer.
    /// </summary>
    public const ulong VectorSize = 2048;
}