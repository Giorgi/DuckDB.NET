namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/appender
    public static partial class Appender
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_create_ext", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderCreateExt(DuckDBNativeConnection connection, string? catalog, string? schema, string table, out DuckDBAppender appender);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_column_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBAppenderColumnCount(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_error_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorData DuckDBAppenderErrorData(DuckDBAppender appender);

        // Maybe [SuppressGCTransition]: reinitializes active chunk — small deallocation + allocation
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_clear")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderClear(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_close")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderClose(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBDestroyAppender(ref IntPtr appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_data_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendDataChunk(DuckDBAppender appender, DuckDBDataChunk chunk);
    }
}
