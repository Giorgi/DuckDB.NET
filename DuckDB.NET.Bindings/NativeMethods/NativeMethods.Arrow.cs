namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/stable/clients/c/api#arrow-interface
    public static partial class Arrow
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_get_arrow_options")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBArrowOptions DuckDBResultGetArrowOptions(ref DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_arrow_options")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyArrowOptions(ref IntPtr arrowOptions);

        // duckdb_error_data duckdb_to_arrow_schema(duckdb_arrow_options, duckdb_logical_type *types,
        //                                          const char **names, idx_t column_count, ArrowSchema *out_schema)
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_to_arrow_schema")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorData DuckDBToArrowSchema(DuckDBArrowOptions arrowOptions, IntPtr types, IntPtr names, ulong columnCount, IntPtr outSchema);

        // duckdb_error_data duckdb_data_chunk_to_arrow(duckdb_arrow_options, duckdb_data_chunk, ArrowArray *out_arrow_array)
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_to_arrow")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorData DuckDBDataChunkToArrow(DuckDBArrowOptions arrowOptions, DuckDBDataChunk chunk, IntPtr outArray);
    }
}
