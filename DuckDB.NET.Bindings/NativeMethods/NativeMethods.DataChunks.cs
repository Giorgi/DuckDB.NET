namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#data-chunk-interface
    public static partial class DataChunks
    {
        // Maybe [SuppressGCTransition]: new DataChunk + Initialize — bounded allocation per column count
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_data_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDataChunk DuckDBCreateDataChunk(IntPtr[] types, ulong count);

        // Maybe [SuppressGCTransition]: delete DataChunk — bounded deallocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_data_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyDataChunk(ref IntPtr chunk);

        // Maybe [SuppressGCTransition]: reinitializes vectors, may deallocate string heaps
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_reset")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDataChunkReset(DuckDBDataChunk chunk);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_get_column_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBDataChunkGetColumnCount(DuckDBDataChunk chunk);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_get_vector")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBDataChunkGetVector(DuckDBDataChunk chunk, long columnIndex);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_get_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBDataChunkGetSize(DuckDBDataChunk chunk);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_set_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBDataChunkSetSize(DuckDBDataChunk chunk, ulong size);
    }
}
