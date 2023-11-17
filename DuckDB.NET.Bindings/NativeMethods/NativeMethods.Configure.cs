using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET;

public partial class NativeMethods
{
    public static class Configure
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_config")]
        public static extern DuckDBState DuckDBCreateConfig(out DuckDBConfig config);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_config_count")]
        public static extern int DuckDBConfigCount();

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_config_flag")]
        public static extern DuckDBState DuckDBGetConfigFlag(int index, out IntPtr name, out IntPtr description);

        [DllImport(DuckDbLibrary, CharSet = CharSet.Ansi, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_set_config")]
        public static extern DuckDBState DuckDBSetConfig(DuckDBConfig config, [MarshalAs(UnmanagedType.LPStr)] string name, [MarshalAs(UnmanagedType.LPStr)] string option);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_config")]
        public static extern void DuckDBDestroyConfig(out IntPtr config);
    }
}