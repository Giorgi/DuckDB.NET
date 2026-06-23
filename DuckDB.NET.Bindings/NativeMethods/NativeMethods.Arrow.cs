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
        public static partial IntPtr DuckDBToArrowSchema(DuckDBArrowOptions arrowOptions, IntPtr types, IntPtr names, ulong columnCount, IntPtr outSchema);

        // duckdb_error_data duckdb_data_chunk_to_arrow(duckdb_arrow_options, duckdb_data_chunk, ArrowArray *out_arrow_array)
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_to_arrow")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBDataChunkToArrow(DuckDBArrowOptions arrowOptions, DuckDBDataChunk chunk, IntPtr outArray);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_error_data_has_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBErrorDataHasError(IntPtr errorData);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_error_data_message")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBErrorDataMessage(IntPtr errorData);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_error_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyErrorData(ref IntPtr errorData);
    }
}
