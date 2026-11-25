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

        // duckdb_error_data duckdb_schema_from_arrow(duckdb_connection, ArrowSchema *schema, duckdb_arrow_converted_schema *out_types)
        // The caller keeps ownership of the schema and must release it.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_schema_from_arrow")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorData DuckDBSchemaFromArrow(DuckDBNativeConnection connection, IntPtr schema, out DuckDBArrowConvertedSchema convertedSchema);

        // duckdb_error_data duckdb_data_chunk_from_arrow(duckdb_connection, ArrowArray *arrow_array,
        //                                                duckdb_arrow_converted_schema, duckdb_data_chunk *out_chunk)
        // DuckDB takes ownership of the array and sets its release callback to NULL; the chunk then releases it when
        // destroyed. v2.0.0 does this whenever the arguments are not null and does not check the array's shape (a
        // column shorter than the array is read past its end); later engines refuse a malformed array first.
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_data_chunk_from_arrow")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorData DuckDBDataChunkFromArrow(DuckDBNativeConnection connection, IntPtr arrowArray, DuckDBArrowConvertedSchema convertedSchema, out DuckDBDataChunk chunk);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_arrow_converted_schema")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyArrowConvertedSchema(ref IntPtr convertedSchema);
    }
}
