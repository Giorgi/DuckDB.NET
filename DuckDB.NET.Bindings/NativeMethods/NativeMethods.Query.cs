namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#query-execution
    public static partial class Query
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, out DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, out DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_result")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyResult(ref DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_fetch_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDataChunk DuckDBFetchChunk(DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_name")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBColumnName(ref DuckDBResult result, long col);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBType DuckDBColumnType(ref DuckDBResult result, long col);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_statement_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBStatementType DuckDBResultStatementType(DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_return_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBResultType DuckDBResultReturnType(DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_logical_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBColumnLogicalType(ref DuckDBResult result, long col);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBColumnCount(ref DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_row_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBRowCount(ref DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_rows_changed")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBRowsChanged(ref DuckDBResult result);

        [Obsolete("Prefer using duckdb_result_get_chunk instead")]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBColumnData(ref DuckDBResult result, long col);

        [Obsolete("Prefer using duckdb_result_get_chunk instead")]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_nullmask_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBNullmaskData(ref DuckDBResult result, long col);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBResultError(ref DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_error_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorType DuckDBResultErrorType(ref DuckDBResult result);
    }
}
