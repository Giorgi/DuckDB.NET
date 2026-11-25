namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#query-execution
    public static partial class Query
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, out DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_result")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyResult(ref DuckDBResult result);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_name")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBColumnName(ref DuckDBResult result, long col);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_column_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBColumnCount(ref DuckDBResult result);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_row_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBRowCount(ref DuckDBResult result);
    }
}
