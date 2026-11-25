namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#prepared-statements
    //https://duckdb.org/docs/api/c/api#bind-values-to-prepared-statements
    public static partial class PreparedStatements
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepare", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

        // Maybe [SuppressGCTransition]: delete PreparedStatementWrapper — small deallocation, may cascade
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_prepare")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyPrepare(ref IntPtr preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_execute_prepared")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);
    }
}
