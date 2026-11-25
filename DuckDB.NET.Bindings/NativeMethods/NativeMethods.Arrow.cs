namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/stable/clients/c/api#arrow-interface
    public static partial class Arrow
    {
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
