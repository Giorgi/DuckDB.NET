using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#openconnect
    public static class Startup
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
        public static extern DuckDBState DuckDBOpen(string? path, out DuckDBDatabase database);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open_ext")]
        public static extern DuckDBState DuckDBOpen(string? path, out DuckDBDatabase database, DuckDBConfig config, out IntPtr error);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
        public static extern void DuckDBClose(out IntPtr database);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
        public static extern DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
        public static extern void DuckDBDisconnect(out IntPtr connection);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_library_version")]
        public static extern IntPtr DuckDBLibraryVersion();
    }
}