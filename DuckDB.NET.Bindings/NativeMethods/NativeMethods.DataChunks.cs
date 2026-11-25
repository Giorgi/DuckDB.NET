namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#data-chunk-interface
    public static partial class DataChunks
    {
        // Maybe [SuppressGCTransition]: delete DataChunk — bounded deallocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_data_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyDataChunk(ref IntPtr chunk);
    }
}
