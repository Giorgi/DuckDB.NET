namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Configuration
    {
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_config_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial nuint DuckDBConfigCount();

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_config_flag")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBGetConfigFlag(nuint index, [MarshalUsing(typeof(DuckDBOwnedStringMarshaller))] out string name, [MarshalUsing(typeof(DuckDBOwnedStringMarshaller))] out string description);
    }
}
