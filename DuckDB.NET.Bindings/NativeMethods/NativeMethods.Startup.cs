namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/stable/clients/c/connect
    public static partial class Startup
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_open", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBOpen(string? path, out DuckDBDatabase database);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_close")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBClose(ref IntPtr database);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_connect")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_disconnect")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDisconnect(ref IntPtr connection);

        // Only the tests use it: they check the V2 library version against it.
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_library_version")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBLibraryVersion();
    }
}
