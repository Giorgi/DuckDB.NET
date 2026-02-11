namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#openconnect
    public static partial class Startup
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_open")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBOpen(SafeUnmanagedMemoryHandle path, out DuckDBDatabase database);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_open", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBOpen(string? path, out DuckDBDatabase database);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_open_ext")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBOpen(SafeUnmanagedMemoryHandle path, out DuckDBDatabase database, DuckDBConfig config, out IntPtr error);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_open_ext", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBOpen(string? path, out DuckDBDatabase database, DuckDBConfig config, out IntPtr error);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_close")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBClose(ref IntPtr database);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_connect")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_disconnect")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDisconnect(ref IntPtr connection);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_interrupt")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBInterrupt(DuckDBNativeConnection connection);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_query_progress")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBQueryProgress DuckDBQueryProgress(DuckDBNativeConnection connection);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_library_version")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBLibraryVersion();
    }
}
