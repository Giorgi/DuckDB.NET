namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Configuration
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_config")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBCreateConfig(out DuckDBConfig config);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_config_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial int DuckDBConfigCount();

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_config_flag")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBGetConfigFlag(int index, out IntPtr name, out IntPtr description);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_set_config", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBSetConfig(DuckDBConfig config, string name, string option);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_config")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyConfig(ref IntPtr config);
    }
}
